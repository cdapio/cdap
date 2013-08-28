package com.continuuity.gateway.v2.handlers.stream;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.v2.GatewayV2Constants;
import com.continuuity.gateway.v2.txmanager.TxManager;
import com.continuuity.streamevent.StreamEventCodec;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class caches stream events and enqueues them in batch.
 */
public class CachedStreamEventCollector extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(CachedStreamEventCollector.class);

  private final Timer flushTimer;

  private final StreamEventCodec serializer = new StreamEventCodec();

  private final CachedStreamEvents cachedStreamEvents;

  private final long flushIntervalMs;

  private final ExecutorService callbackExecutorService;

  @Inject
  public CachedStreamEventCollector(CConfiguration cConfig, OperationExecutor opex,
                                    QueueClientFactory queueClientFactory) {
    this.flushTimer = new Timer("stream-rest-flush-thread", true);

    int maxCachedEventsPerStream = cConfig.getInt(GatewayV2Constants.ConfigKeys.MAX_CACHED_EVENTS_PER_STREAM_NUM,
                                                  GatewayV2Constants.DEFAULT_MAX_CACHED_EVENTS_PER_STREAM_NUM);
    int maxCachedEvents = cConfig.getInt(GatewayV2Constants.ConfigKeys.MAX_CACHED_STREAM_EVENTS_NUM,
                                         GatewayV2Constants.DEFAULT_MAX_CACHED_STREAM_EVENTS_NUM);
    long maxCachedSizeBytes = cConfig.getLong(GatewayV2Constants.ConfigKeys.MAX_CACHED_STREAM_EVENTS_BYTES,
                                              GatewayV2Constants.DEFAULT_MAX_CACHED_STREAM_EVENTS_BYTES);
    this.flushIntervalMs = cConfig.getLong(GatewayV2Constants.ConfigKeys.STREAM_EVENTS_FLUSH_INTERVAL_MS,
                                           GatewayV2Constants.DEFAULT_STREAM_EVENTS_FLUSH_INTERVAL_MS);

    this.callbackExecutorService = Executors.newFixedThreadPool(5,
                                                                new ThreadFactoryBuilder()
                                                                  .setDaemon(true)
                                                                  .setNameFormat("stream-rest-callback-thread")
                                                                  .build()
    );

    this.cachedStreamEvents = new CachedStreamEvents(opex, queueClientFactory, callbackExecutorService,
                                                     maxCachedSizeBytes, maxCachedEvents,
                                                     maxCachedEventsPerStream);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting up {}", this.getClass().getSimpleName());
    flushTimer.scheduleAtFixedRate(
      new TimerTask() {
        @Override
        public void run() {
          LOG.debug("Running flush from timer task.");
          cachedStreamEvents.flush(false);
          LOG.debug("Done running flush from timer task.");
        }
      },
      flushIntervalMs, flushIntervalMs
    );
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down {}", this.getClass().getSimpleName());
    flushTimer.cancel();
    cachedStreamEvents.flush(true);
    callbackExecutorService.shutdown();
  }

  /**
   * Used to enqueue a StreamEvent.
   * @param event StreamEvent to enqueue.
   * @param accountId accountId of the entity making the call.
   * @param callback Callback to be called after enqueuing the event
   * @throws Exception
   */
  public void consume(StreamEvent event, String accountId, FutureCallback<Void> callback)
    throws Exception {
    byte[] bytes = serializer.encodePayload(event);
    if (bytes == null) {
      LOG.trace("Could not serialize event: {}", event);
      throw new Exception("Could not serialize event: " + event);
    }

    String destination = event.getHeaders().get(Constants.HEADER_DESTINATION_STREAM);
    if (destination == null) {
      LOG.trace("Enqueuing an event that has no destination. Using 'default' instead.");
      destination = "default";
    }

    QueueName queueName = QueueName.fromStream(accountId, destination);
    cachedStreamEvents.put(queueName, new QueueEntry(bytes), callback);
  }

  /**
   * Caches StreamEvents and flushes them periodically.
   */
  private static final class CachedStreamEvents {
    private static final Logger LOG = LoggerFactory.getLogger(CachedStreamEvents.class);

    private final LoadingCache<QueueName, ProducerStreamEntries> eventCache;

    private final long maxCachedSizeBytes;
    private final long maxCachedEvents;
    private final AtomicLong cachedBytes = new AtomicLong(0);
    private final AtomicInteger cachedNumEntries = new AtomicInteger(0);

    private final AtomicBoolean flushRunning = new AtomicBoolean(false);

    private CachedStreamEvents(final OperationExecutor opex, final QueueClientFactory queueClientFactory,
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
                                               opex, callbackExecutorService, cachedBytes, cachedNumEntries);
            }
          });
    }

    /**
     * Add a QueueEntry to the Queue Producer list so that it can be flushed later.
     * @param queueName queue name
     * @param queueEntry queue entry
     * @param callback callback to be called after the queue entry is enqueued.
     * @throws ExecutionException
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
                                   OperationExecutor opex, ExecutorService callbackExecutorService,
                                   AtomicLong cachedBytes, AtomicInteger cachedNumEntries) {
        this.producer = producer;
        this.txManager = new TxManager(opex, (TransactionAware) producer);
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

      public void flush() {
        List<StreamEntry> entries = Lists.newArrayListWithExpectedSize(streamEntries.size());
        SettableFuture<Void> future = SettableFuture.create();
        StreamEntryToQueueEntryFunction transformer =
          new StreamEntryToQueueEntryFunction(future, callbackExecutorService);

        synchronized (txManager) {
          try {
            streamEntries.drainTo(entries);
            if (entries.isEmpty()) {
              return;
            }

            txManager.start();
            producer.enqueue(Iterables.transform(entries, transformer));

            // Commit
            txManager.commit();
            future.set(null);

          } catch (Throwable e) {
            LOG.error("Exception when trying to enqueue with producer {}. Aborting txn...", producer, e);

            try {
              txManager.abort();
            } catch (OperationException e1) {
              LOG.error("Exception while aborting txn", e1);
            } finally {
              future.setException(e);
            }
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
      }

      @Override
      public void close() throws IOException {
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
  }

  /**
   * Represents a stream entry with a callback to be called after enqueuing the entry.
   */
  private static class StreamEntry {
    private final QueueEntry queueEntry;
    private final FutureCallback<Void> callback;

    private StreamEntry(QueueEntry queueEntry, FutureCallback<Void> callback) {
      this.queueEntry = queueEntry;
      this.callback = callback;
    }

    public QueueEntry getQueueEntry() {
      return queueEntry;
    }

    public FutureCallback<Void> getCallback() {
      return callback;
    }
  }

  private static final class StreamEntryToQueueEntryFunction implements Function<StreamEntry, QueueEntry> {
    private long numBytes = 0;
    private int numEntries = 0;

    private final ListenableFuture<Void> future;
    private final ExecutorService callbackExecutorService;

    private StreamEntryToQueueEntryFunction(ListenableFuture<Void> future, ExecutorService callbackExecutorService) {
      this.future = future;
      this.callbackExecutorService = callbackExecutorService;
    }

    @Override
    public QueueEntry apply(StreamEntry input) {
      Futures.addCallback(future, input.getCallback(), callbackExecutorService);

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

}
