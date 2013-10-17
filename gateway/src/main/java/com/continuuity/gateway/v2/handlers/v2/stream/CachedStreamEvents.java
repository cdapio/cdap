package com.continuuity.gateway.v2.handlers.v2.stream;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.queue.QueueEntry;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.queue.QueueMetrics;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Caches StreamEvents and flushes them periodically.
 */
final class CachedStreamEvents {
  private static final Logger LOG = LoggerFactory.getLogger(CachedStreamEvents.class);

  private final LoadingCache<QueueName, ProducerStreamEntries> eventCache;

  private final long maxCachedSizeBytes;
  private final long maxCachedEvents;
  private final AtomicLong cachedBytes = new AtomicLong(0);
  private final AtomicInteger cachedNumEntries = new AtomicInteger(0);

  private MetricsCollectionService metricsCollectionService;

  CachedStreamEvents(final TransactionSystemClient txClient, final QueueClientFactory queueClientFactory,
                     final ExecutorService callbackExecutorService,
                     long maxCachedSizeBytes, int maxCachedEvents,
                     final int maxCachedEventsPerStream) {
    this.maxCachedSizeBytes = maxCachedSizeBytes;
    this.maxCachedEvents = maxCachedEvents;

    this.eventCache = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .removalListener(
        new RemovalListener<QueueName, ProducerStreamEntries>() {
          @Override
          public void onRemoval(RemovalNotification<QueueName, ProducerStreamEntries> notification) {
            ProducerStreamEntries producerStreamEntries = notification.getValue();
            if (producerStreamEntries != null) {
              producerStreamEntries.flush(true);
              try {
                producerStreamEntries.close();
              } catch (IOException e) {
                LOG.error("Exception while closing producer {}", producerStreamEntries);
              }
            }
          }
        }
      )
      .build(
        new CacheLoader<QueueName, ProducerStreamEntries>() {
          @Override
          public ProducerStreamEntries load(final QueueName key) throws Exception {
            Queue2Producer producer = metricsCollectionService == null ? queueClientFactory.createProducer(key) :
              queueClientFactory.createProducer(
                key,
                new QueueMetrics() {
                  MetricsCollector collector =
                    metricsCollectionService.getCollector(MetricsScope.REACTOR, Constants.Gateway.METRICS_CONTEXT, "0");

                  @Override
                  public void emitEnqueue(int count) {
                    collector.gauge("collect.events", count, key.getSimpleName());
                  }

                  @Override
                  public void emitEnqueueBytes(int bytes) {
                    collector.gauge("collect.bytes", bytes, key.getSimpleName());
                  }
                });

            return new ProducerStreamEntries(producer,
                                             new ArrayBlockingQueue<StreamEntry>(maxCachedEventsPerStream),
                                             txClient, callbackExecutorService, cachedBytes, cachedNumEntries);
          }
        });
  }

  public void setMetricsCollectionService(MetricsCollectionService metricsCollectionService) {
    LOG.info("Setting metricsCollectionService");
    this.metricsCollectionService = metricsCollectionService;
  }

  /**
   * Add a QueueEntry to the Queue Producer list so that it can be flushed later.
   * @param queueName queue name
   * @param queueEntry queue entry
   * @param callback callback to be called after the queue entry is enqueued.
   * @throws java.util.concurrent.ExecutionException
   * @throws InterruptedException
   */
  public void put(QueueName queueName, QueueEntry queueEntry, FutureCallback<Void> callback)
    throws ExecutionException, InterruptedException {

    eventCache.get(queueName).enqueue(new StreamEntry(queueEntry, callback));

    long cachedSize = cachedBytes.get();
    int cachedEntries = cachedNumEntries.get();

    // If reached limit, then flush
    if (cachedEntries >= maxCachedEvents || cachedSize >= maxCachedSizeBytes) {
      LOG.trace("Exceeded limit - cachedEntries={}, cachedSize={}", cachedEntries, cachedSize);
      flush(false);
    }
  }

  /**
   * Performs a flush of cached stream events.
   * @param mustFlush if false does not run flush if another thread is already running flush,
   *                  if true will run a flush even if another one is running.
   */
  public void flush(boolean mustFlush) {
    LOG.trace("Running flush. Must flush={}, cachedEntries={}, cachedSize={}",
              mustFlush, cachedNumEntries.get(), cachedBytes.get());

    for (Map.Entry<QueueName, ProducerStreamEntries> entry : eventCache.asMap().entrySet()) {
      entry.getValue().flush(mustFlush);
    }

    LOG.trace("Done running flush. cachedEntries={}, cachedSize={}", cachedNumEntries.get(), cachedBytes.get());
  }

  /**
   * Contains a Queue Producer along with the stream entries belonging to the queue.
   */
  private static class ProducerStreamEntries implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerStreamEntries.class);

    private final Queue2Producer producer;
    private final TransactionContext txContext;
    private final BlockingQueue<StreamEntry> streamEntries;

    private final ExecutorService callbackExecutorService;

    private final AtomicLong cachedBytes;
    private final AtomicInteger cachedNumEntries;

    private final ReentrantLock flushLock = new ReentrantLock();

    public ProducerStreamEntries(Queue2Producer producer, BlockingQueue<StreamEntry> streamEntries,
                                 TransactionSystemClient txClient, ExecutorService callbackExecutorService,
                                 AtomicLong cachedBytes, AtomicInteger cachedNumEntries) {
      this.producer = producer;
      this.txContext = new TransactionContext(txClient, (TransactionAware) producer);
      this.streamEntries = streamEntries;

      this.callbackExecutorService = callbackExecutorService;

      this.cachedBytes = cachedBytes;
      this.cachedNumEntries = cachedNumEntries;
    }

    public void enqueue(StreamEntry streamEntry) {
      long timeoutNanos = 1;

      for (int i = 0; i < 10; ++i) {
        try {
          if (streamEntries.offer(streamEntry, timeoutNanos, TimeUnit.NANOSECONDS)) {
            cachedBytes.addAndGet(streamEntry.getQueueEntry().getData().length);
            cachedNumEntries.incrementAndGet();
            return;
          } else {
            LOG.trace("Not able to add to producer queue, flushing");
            if (!flush(false)) {
              timeoutNanos = TimeUnit.MILLISECONDS.toNanos(200);
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

      throw new IllegalStateException(String.format("Cannot add stream entry using producer %s", producer));
    }

    public boolean flush(boolean mustFlush) {
      if (mustFlush) {
        flushLock.lock();
      } else {
        if (!flushLock.tryLock()) {
          // Some other thread is running flush
          LOG.trace("Some other thread is running flush, returning");
          return false;
        }
      }

      LOG.trace("Got flush lock, running flush");

      try {
        List<StreamEntry> entries = Lists.newArrayListWithExpectedSize(streamEntries.size());
        streamEntries.drainTo(entries);
        if (entries.isEmpty()) {
          LOG.trace("Nothing to flush for producer {}", producer);
          return true;
        }

        CallbackNotifier callbackNotifier =
          new CallbackNotifier(callbackExecutorService,
                               Iterables.transform(entries, STREAM_ENTRY_FUTURE_CALLBACK_FUNCTION));
        StreamEntryToQueueEntryFunction transformer = new StreamEntryToQueueEntryFunction();

        try {
          txContext.start();

          try {
            producer.enqueue(Iterables.transform(entries, transformer));

            // Commit
            txContext.finish();

            // Notify callbacks
            callbackNotifier.notifySuccess();
            LOG.trace("Flushed {} events with producer {}", entries.size(), producer);
          } catch (Throwable e) {
            LOG.error("Exception when trying to enqueue with producer {}. Aborting txn...", producer, e);

            try {
              txContext.abort();
            } catch (TransactionFailureException e1) {
              LOG.error("Exception while aborting txn", e1);
            } finally {
              // Notify callbacks
              callbackNotifier.notifyFailure(e);
            }
          }
        } catch (TransactionFailureException e) {
          LOG.error("Caught exception", e);
          callbackNotifier.notifyFailure(e);
        } finally {
          int numEntries = entries.size();
          cachedNumEntries.addAndGet(-1 * numEntries);

          long numBytes = transformer.getNumBytes();
          if (numEntries != transformer.getNumEntries()) {
            numBytes = numBytesInList(entries);
          }
          cachedBytes.addAndGet(-1 * numBytes);
        }
      } finally {
        flushLock.unlock();
      }
      return true;
    }

    @Override
    public void close() throws IOException {
      flushLock.lock();
      try {
        if (producer instanceof Closeable) {
          ((Closeable) producer).close();
        }
      } finally {
        flushLock.unlock();
      }
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("producer", producer)
        .add("txContext", txContext)
        .toString();
    }

    private long numBytesInList(List<StreamEntry> list) {
      long numBytes = 0;
      for (StreamEntry entry : list) {
        numBytes += entry.getQueueEntry().getData().length;
      }
      return numBytes;
    }
  }

  private static final class StreamEntryToQueueEntryFunction implements Function<StreamEntry, QueueEntry> {
    private long numBytes = 0;
    private int numEntries = 0;

    @Override
    public QueueEntry apply(StreamEntry input) {
      numBytes += input.getQueueEntry().getData().length;
      ++numEntries;

      return input.getQueueEntry();
    }

    public long getNumBytes() {
      return numBytes;
    }

    public int getNumEntries() {
      return numEntries;
    }
  }

  private static final Function<StreamEntry, FutureCallback<Void>> STREAM_ENTRY_FUTURE_CALLBACK_FUNCTION =
    new Function<StreamEntry, FutureCallback<Void>>() {
      @Override
      public FutureCallback<Void> apply(StreamEntry input) {
        return input.getCallback();
      }
    };
}
