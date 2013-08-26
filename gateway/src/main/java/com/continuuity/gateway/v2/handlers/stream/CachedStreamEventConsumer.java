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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class caches stream events and enqueues them in batch.
 */
public class CachedStreamEventConsumer extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(CachedStreamEventConsumer.class);

  private final OperationExecutor opex;
  private final QueueClientFactory queueClientFactory;

  private final Timer flushTimer;

  private final StreamEventCodec serializer = new StreamEventCodec();

  private final CachedStreamEvents cachedStreamEvents;

  private final int maxCachedEvents;
  private final int maxCachedEventsPerStream;
  private final long maxCachedSizeBytes;
  private final long flushIntervalMs;

  private final ExecutorService callbackExecutorService;

  @Inject
  public CachedStreamEventConsumer(CConfiguration cConfig, OperationExecutor opex,
                                   QueueClientFactory queueClientFactory) {
    this.opex = opex;
    this.queueClientFactory = queueClientFactory;
    this.flushTimer = new Timer("stream-rest-flush-thread", true);

    this.maxCachedEvents = cConfig.getInt(GatewayV2Constants.ConfigKeys.MAX_CACHED_STREAM_EVENTS_NUM,
                                          GatewayV2Constants.DEFAULT_MAX_CACHED_STREAM_EVENTS_NUM);
    this.maxCachedEventsPerStream = cConfig.getInt(GatewayV2Constants.ConfigKeys.MAX_CACHED_EVENTS_PER_STREAM_NUM,
                                                   GatewayV2Constants.DEFAULT_MAX_CACHED_EVENTS_PER_STREAM_NUM);
    this.maxCachedSizeBytes = cConfig.getLong(GatewayV2Constants.ConfigKeys.MAX_CACHED_STREAM_EVENTS_BYTES,
                                              GatewayV2Constants.DEFAULT_MAX_CACHED_STREAM_EVENTS_BYTES);
    this.flushIntervalMs = cConfig.getLong(GatewayV2Constants.ConfigKeys.STREAM_EVENTS_FLUSH_INTERVAL_MS,
                                           GatewayV2Constants.DEFAULT_STREAM_EVENTS_FLUSH_INTERVAL_MS);

    this.cachedStreamEvents = new CachedStreamEvents();
    this.callbackExecutorService = Executors.newFixedThreadPool(5,
                                                                new ThreadFactoryBuilder()
                                                                  .setDaemon(true)
                                                                  .setNameFormat("stream-rest-callback-thread")
                                                                  .build()
    );
  }

  @Override
  protected void startUp() throws Exception {
    flushTimer.scheduleAtFixedRate(
      new TimerTask() {
        @Override
        public void run() {
          cachedStreamEvents.flush();
        }
      },
      flushIntervalMs, flushIntervalMs
    );
  }

  @Override
  protected void shutDown() throws Exception {
    flushTimer.cancel();
    cachedStreamEvents.flush();
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
  private final class CachedStreamEvents {
    private final LoadingCache<QueueName, ProducerStreamEntries> eventCache;
    private final BlockingQueue<Map.Entry<QueueName, ProducerStreamEntries>> removalNotifications;

    private final TxManager txManager = new TxManager(opex);

    private final AtomicLong cachedBytes = new AtomicLong(0);
    private final AtomicInteger cachedNumEntries = new AtomicInteger(0);
    private final Lock flushLock = new ReentrantLock();

    private CachedStreamEvents() {
      this.removalNotifications = new LinkedBlockingQueue<Map.Entry<QueueName, ProducerStreamEntries>>();
      this.eventCache = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.HOURS)
        .removalListener(
          new RemovalListener<QueueName, ProducerStreamEntries>() {
            @Override
            public void onRemoval(RemovalNotification<QueueName, ProducerStreamEntries> notification) {
              //noinspection ConstantConditions
              if (notification.getValue() != null && !notification.getValue().isEmpty()) {
                // Will need to be committed during the next flush
                removalNotifications.add(notification);
              }
            }
          }
        )
        .build(
          new CacheLoader<QueueName, ProducerStreamEntries>() {
            @Override
            public ProducerStreamEntries load(QueueName key) throws Exception {
              Queue2Producer producer = queueClientFactory.createProducer(key);
              txManager.add((TransactionAware) producer);
              return new ProducerStreamEntries(producer, new ArrayBlockingQueue<StreamEntry>(maxCachedEventsPerStream));
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

      if (!eventCache.get(queueName).offer(new StreamEntry(queueEntry, callback))) {
        // Flush and try again
        flush();
        if (!eventCache.get(queueName).offer(new StreamEntry(queueEntry, callback))) {
          throw new IllegalStateException(String.format("Cannot add stream entry into queue %s", queueName));
        }
      }

      long cachedSize = cachedBytes.addAndGet(queueEntry.getData().length);
      int cachedEntries = cachedNumEntries.incrementAndGet();
      if (cachedEntries >= maxCachedEvents || cachedSize >= maxCachedSizeBytes) {
        checkedFlush();
      }
    }

    /**
     * Performs a flush of cached stream events making sure that only one thread does the flush.
     */
    public void checkedFlush() {
      // Only one thread needs to run flush
      if (flushLock.tryLock()) {
        try {
          flush();
        } finally {
          flushLock.unlock();
        }
      }
    }

    /**
     * Flushes the cached stream events. This method blocks if some other thread is already running flush.
     */
    private void flush() {
      flushLock.lock();
      try {
        Set<Map.Entry<QueueName, ProducerStreamEntries>> removalEntries = ImmutableSet.of();
        SettableFuture<Void> future = SettableFuture.create();
        try {
          txManager.start();

          // Get cache entries that were evicted, if any
          if (!removalNotifications.isEmpty()) {
            int removalSize = removalNotifications.size();
            removalEntries = Sets.newHashSetWithExpectedSize(removalSize);
            removalNotifications.drainTo(removalEntries, removalSize);
          }

          // Enqueue
          List<StreamEntry> entries = Lists.newArrayList();
          for (Map.Entry<QueueName, ProducerStreamEntries> entry :
            Iterables.concat(eventCache.asMap().entrySet(), removalEntries)) {
            ProducerStreamEntries producerStreamEntries = entry.getValue();
            if (producerStreamEntries.isEmpty()) {
              continue;
            }

            int size = producerStreamEntries.size();
            entries.clear();
            producerStreamEntries.drainTo(entries, size);
            if (entries.isEmpty()) {
              continue;
            }

            cachedNumEntries.addAndGet(-1 * entries.size());
            cachedBytes.addAndGet(-1 * numBytesInList(entries));

            addCallbacksToFuture(future, entries);
            producerStreamEntries.getProducer().enqueue(
              Iterables.transform(entries, STREAM_ENTRY_QUEUE_ENTRY_TRANSFORMER));
          }

          // Commit
          txManager.commit();
          future.set(null);
        } catch (Exception e){
          LOG.error("Exception while committing stream events. Aborting transaction", e);
          future.setException(e);
          txManager.abort();
        } finally {
          // Remove producers from removalNotifications
          for (Map.Entry<QueueName, ProducerStreamEntries> entry : removalEntries) {
            Queue2Producer producer = entry.getValue().getProducer();
            if (producer instanceof Closeable) {
              try {
                ((Closeable) producer).close();
              } catch (IOException e) {
                LOG.error("Caught exception while closing producer {}", producer, e);
              }
            }
            txManager.remove((TransactionAware) producer);
          }
        }

      } catch (OperationException e) {
        LOG.error("Exception while aborting stream events transaction", e);
      } finally {
        flushLock.unlock();
      }
    }

    private long numBytesInList(List<StreamEntry> list) {
      long numBytes = 0;
      for (StreamEntry entry : list) {
        numBytes += entry.getQueueEntry().getData().length;
      }
      return numBytes;
    }

    /**
     * Contains a Queue Producer along with the stream entries belonging to the queue.
     */
    private class ProducerStreamEntries {
      private final Queue2Producer producer;
      private final BlockingQueue<StreamEntry> streamEntries;

      public ProducerStreamEntries(Queue2Producer producer, BlockingQueue<StreamEntry> streamEntries) {
        this.producer = producer;
        this.streamEntries = streamEntries;
      }

      public boolean offer(StreamEntry streamEntry) {
        return streamEntries.offer(streamEntry);
      }
      
      public void drainTo(Collection<StreamEntry> collection, int maxEntries) {
        streamEntries.drainTo(collection, maxEntries);
      }

      public boolean isEmpty() {
        return streamEntries.isEmpty();
      }

      public int size() {
        return streamEntries.size();
      }

      public Queue2Producer getProducer() {
        return producer;
      }
    }
  }

  /**
   * Represents a stream entry with a callback to be called after enqueuing the entry.
   */
  private class StreamEntry {
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

  private static final Function<StreamEntry, QueueEntry> STREAM_ENTRY_QUEUE_ENTRY_TRANSFORMER =
    new Function<StreamEntry, QueueEntry>() {
      @Nullable
      @Override
      public QueueEntry apply(@Nullable StreamEntry input) {
        return input == null ? null : input.getQueueEntry();
      }
    };

  private void addCallbacksToFuture(ListenableFuture<Void> future, Iterable<StreamEntry> entries) {
    for (StreamEntry entry : entries) {
      Futures.addCallback(future, entry.getCallback(), callbackExecutorService);
    }
  }
}
