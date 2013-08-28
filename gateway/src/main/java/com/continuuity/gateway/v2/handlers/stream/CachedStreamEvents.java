package com.continuuity.gateway.v2.handlers.stream;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.gateway.v2.txmanager.TxManager;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

  private final AtomicBoolean flushRunning = new AtomicBoolean(false);

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
              producerStreamEntries.flush();
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
          public ProducerStreamEntries load(QueueName key) throws Exception {
            Queue2Producer producer = queueClientFactory.createProducer(key);
            return new ProducerStreamEntries(producer,
                                             new ArrayBlockingQueue<StreamEntry>(maxCachedEventsPerStream),
                                             txClient, callbackExecutorService, cachedBytes, cachedNumEntries);
          }
        });
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
    long cachedSize = cachedBytes.addAndGet(queueEntry.getData().length);
    int cachedEntries = cachedNumEntries.incrementAndGet();

    // If reached limit, then flush
    if (cachedEntries >= maxCachedEvents || cachedSize >= maxCachedSizeBytes) {
      LOG.debug("Exceeded limit - cachedEntries={}, cachedSize={}", cachedEntries, cachedSize);
      flush(false);
    }
  }

  /**
   * Performs a flush of cached stream events.
   * @param mustFlush if false does not run flush if another thread is already running flush,
   *                  if true will run a flush even if another one is running.
   */
  public void flush(boolean mustFlush) {
    if (!mustFlush && flushRunning.compareAndSet(false, true)) {
      return;
    }

    LOG.debug("Running flush. Must flush={}", mustFlush);
    try {
      for (Map.Entry<QueueName, ProducerStreamEntries> entry : eventCache.asMap().entrySet()) {
        entry.getValue().flush();
      }
    } finally {
      flushRunning.set(false);
    }
    LOG.debug("Done running flush");
  }

  /**
   * Contains a Queue Producer along with the stream entries belonging to the queue.
   */
  private static class ProducerStreamEntries implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerStreamEntries.class);

    private final Queue2Producer producer;
    private final TxManager txManager;
    private final BlockingQueue<StreamEntry> streamEntries;

    private final ExecutorService callbackExecutorService;

    private final AtomicLong cachedBytes;
    private final AtomicInteger cachedNumEntries;

    public ProducerStreamEntries(Queue2Producer producer, BlockingQueue<StreamEntry> streamEntries,
                                 TransactionSystemClient txClient, ExecutorService callbackExecutorService,
                                 AtomicLong cachedBytes, AtomicInteger cachedNumEntries) {
      this.producer = producer;
      this.txManager = new TxManager(txClient, (TransactionAware) producer);
      this.streamEntries = streamEntries;

      this.callbackExecutorService = callbackExecutorService;

      this.cachedBytes = cachedBytes;
      this.cachedNumEntries = cachedNumEntries;
    }

    public void enqueue(StreamEntry streamEntry) {
      for (int i = 0; i < 10; ++i) {
        if (streamEntries.offer(streamEntry)) {
          cachedBytes.addAndGet(streamEntry.getQueueEntry().getData().length);
          cachedNumEntries.incrementAndGet();
          return;
        } else {
          flush();
        }
      }

      throw new IllegalStateException(String.format("Cannot add stream entry using producer %s", producer));
    }

    public synchronized void flush() {
      List<StreamEntry> entries = Lists.newArrayListWithExpectedSize(streamEntries.size());
      streamEntries.drainTo(entries);
      if (entries.isEmpty()) {
        return;
      }

      CallbackNotifier callbackNotifier =
        new CallbackNotifier(callbackExecutorService,
                             Iterables.transform(entries, STREAM_ENTRY_FUTURE_CALLBACK_FUNCTION));
      StreamEntryToQueueEntryFunction transformer = new StreamEntryToQueueEntryFunction();

      try {
        txManager.start();

        try {
          producer.enqueue(Iterables.transform(entries, transformer));

          // Commit
          txManager.commit();

          // Notify callbacks
          callbackNotifier.notifySuccess();
          LOG.debug("Flushed {} events with producer {}", entries.size(), producer);
        } catch (Throwable e) {
          LOG.error("Exception when trying to enqueue with producer {}. Aborting txn...", producer, e);

          try {
            txManager.abort();
          } catch (OperationException e1) {
            LOG.error("Exception while aborting txn", e1);
          } finally {
            // Notify callbacks
            callbackNotifier.notifyFailure(e);
          }
        }
      } catch (OperationException e) {
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
    }

    @Override
    public synchronized void close() throws IOException {
      if (producer instanceof Closeable) {
        ((Closeable) producer).close();
      }
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("producer", producer)
        .add("txManager", txManager)
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
