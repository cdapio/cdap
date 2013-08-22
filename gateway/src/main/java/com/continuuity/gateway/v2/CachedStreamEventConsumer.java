package com.continuuity.gateway.v2;

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
import com.continuuity.streamevent.StreamEventCodec;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class caches stream events and enqueues them in batch.
 */
public class CachedStreamEventConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(CachedStreamEventConsumer.class);

  private final OperationExecutor opex;
  private final QueueClientFactory queueClientFactory;

  private final Timer timer;

  private final StreamEventCodec serializer = new StreamEventCodec();

  private final CachedStreamEvents cachedStreamEvents;

  private final int maxCachedEvents;
  private final int maxCachedEventsPerStream;
  private final long maxCachedSizeBytes;
  private final long flushIntervalMs;

  @Inject
  public CachedStreamEventConsumer(CConfiguration cConfig, OperationExecutor opex,
                                   QueueClientFactory queueClientFactory) {
    this.opex = opex;
    this.queueClientFactory = queueClientFactory;
    this.timer = new Timer(String.format("%s-thread", getClass().getCanonicalName()), true);

    this.maxCachedEvents = cConfig.getInt(GatewayConstants.ConfigKeys.MAX_CACHED_STREAM_EVENTS_NUM,
                                          GatewayConstants.DEFAULT_MAX_CACHED_STREAM_EVENTS_NUM);
    this.maxCachedEventsPerStream = cConfig.getInt(GatewayConstants.ConfigKeys.MAX_CACHED_EVENTS_PER_STREAM_NUM,
                                                   GatewayConstants.DEFAULT_MAX_CACHED_EVENTS_PER_STREAM_NUM);
    this.maxCachedSizeBytes = cConfig.getLong(GatewayConstants.ConfigKeys.MAX_CACHED_STREAM_EVENTS_BYTES,
                                              GatewayConstants.DEFAULT_MAX_CACHED_STREAM_EVENTS_BYTES);
    this.flushIntervalMs = cConfig.getLong(GatewayConstants.ConfigKeys.STREAM_EVENTS_FLUSH_INTERVAL_MS,
                                           GatewayConstants.DEFAULT_STREAM_EVENTS_FLUSH_INTERVAL_MS);

    this.cachedStreamEvents = new CachedStreamEvents();
  }

  public void start() {
    timer.scheduleAtFixedRate(
      new TimerTask() {
        @Override
        public void run() {
          cachedStreamEvents.flush();
        }
      },
      flushIntervalMs, flushIntervalMs
    );
  }

  public void stop() {
    timer.cancel();
    cachedStreamEvents.flush();
  }

  public ListenableFuture<Void> consume(StreamEvent event, String accountId) throws Exception {
    byte[] bytes = serializer.encodePayload(event);
    if (bytes == null) {
      LOG.warn("Could not serialize event: {}", event);
      throw new Exception("Could not serialize event: " + event);
    }

    String destination = event.getHeaders().get(Constants.HEADER_DESTINATION_STREAM);
    if (destination == null) {
      LOG.warn("Enqueuing an event that has no destination. Using 'default' instead.");
      destination = "default";
    }

    QueueName queueName = QueueName.fromStream(accountId, destination);
    return cachedStreamEvents.put(queueName, new QueueEntry(bytes));
  }

  private final class CachedStreamEvents {
    private final LoadingCache<QueueName, CachedEntry> eventCache;
    private final BlockingQueue<Map.Entry<QueueName, CachedEntry>> removalNotifications;

    private final TxManager txManager = new TxManager(opex);

    private final SettableFuture<Void> future = SettableFuture.create();
    private final AtomicLong cachedBytes = new AtomicLong(0);
    private final AtomicInteger cachedNumEntries = new AtomicInteger(0);
    private final Lock flushLock = new ReentrantLock();

    private CachedStreamEvents() {
      this.removalNotifications = new LinkedBlockingQueue<Map.Entry<QueueName, CachedEntry>>();
      this.eventCache = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.HOURS)
        .removalListener(
          new RemovalListener<QueueName, CachedEntry>() {
            @Override
            public void onRemoval(RemovalNotification<QueueName, CachedEntry> notification) {
              //noinspection ConstantConditions
              if (notification.getValue() != null && !notification.getValue().isEmpty()) {
                // Will need to be comitted during the next flush
                removalNotifications.add(notification);
              }
            }
          }
        )
        .build(
          new CacheLoader<QueueName, CachedEntry>() {
            @Override
            public CachedEntry load(QueueName key) throws Exception {
              Queue2Producer producer = queueClientFactory.createProducer(key);
              txManager.add((TransactionAware) producer);
              return new CachedEntry(producer, new ArrayBlockingQueue<QueueEntry>(maxCachedEventsPerStream));
            }
          });
    }

    public ListenableFuture<Void> put(QueueName queueName, QueueEntry queueEntry)
      throws ExecutionException, InterruptedException {

      int tries = 0;
      while (!eventCache.get(queueName).offer(queueEntry)) {
        flush();
        ++tries;

        if (tries > 4) {
          throw new IllegalStateException(String.format("Cannot add stream entry into queue %s", queueName));
        }
      }

      long cachedSize = cachedBytes.addAndGet(queueEntry.getData().length);
      int cachedEntries = cachedNumEntries.incrementAndGet();
      if (cachedEntries >= maxCachedEvents || cachedSize >= maxCachedSizeBytes) {
        checkedFlush();
      }
      return future;
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
     * Flushes the cached stream events. This method can be called concurrently from multiple threads.
     */
    private void flush() {
      flushLock.lock();
      try {
        Set<Map.Entry<QueueName, CachedEntry>> removalEntries = ImmutableSet.of();
        try {
          txManager.start();

          // Get cache entries that were evicted, if any
          if (!removalNotifications.isEmpty()) {
            int removalSize = removalNotifications.size();
            removalEntries = Sets.newHashSetWithExpectedSize(removalSize);
            removalNotifications.drainTo(removalEntries, removalSize);
          }

          // Enqueue
          for (Map.Entry<QueueName, CachedEntry> entry :
            Iterables.concat(eventCache.asMap().entrySet(), removalEntries)) {
            CachedEntry cachedEntry = entry.getValue();
            if (cachedEntry.isEmpty()) {
              continue;
            }

            int size = cachedEntry.size();
            List<QueueEntry> entries = Lists.newArrayListWithCapacity(size);
            cachedEntry.drainTo(entries, size);
            if (entries.isEmpty()) {
              continue;
            }

            cachedNumEntries.addAndGet(-1 * entries.size());
            cachedBytes.addAndGet(-1 * numBytesInList(entries));
            cachedEntry.getProducer().enqueue(entries);
          }

          // Commit
          txManager.commit();
          future.set(null);
        } catch (Exception e){
          LOG.error("Exception while committing stream events. Aborting transaction", e);
          future.setException(e);
          txManager.abort();
        }

        // Remove producers from removalNotifications
        for (Map.Entry<QueueName, CachedEntry> entry : removalEntries) {
          txManager.remove((TransactionAware) entry.getValue().getProducer());
        }
      } catch (OperationException e) {
        LOG.error("Exception while aborting stream events transaction", e);
      } finally {
        flushLock.unlock();
      }
    }

    private long numBytesInList(List<QueueEntry> list) {
      long numBytes = 0;
      for (QueueEntry queueEntry : list) {
        numBytes += queueEntry.getData().length;
      }
      return numBytes;
    }

    private class CachedEntry {
      private final Queue2Producer producer;
      private final BlockingQueue<QueueEntry> queueEntries;

      public CachedEntry(Queue2Producer producer, BlockingQueue<QueueEntry> queueEntries) {
        this.producer = producer;
        this.queueEntries = queueEntries;
      }

      public boolean offer(QueueEntry queueEntry) {
        return queueEntries.offer(queueEntry);
      }
      
      public void drainTo(Collection<QueueEntry> collection, int maxEntries) {
        queueEntries.drainTo(collection, maxEntries);
      }

      public boolean isEmpty() {
        return queueEntries.isEmpty();
      }

      public int size() {
        return queueEntries.size();
      }

      public Queue2Producer getProducer() {
        return producer;
      }
    }
  }
}
