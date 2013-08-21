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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    private final LoadingCache<QueueName, BlockingQueue<QueueEntry>> eventCache;

    private final SettableFuture<Void> future = SettableFuture.create();
    private final AtomicLong cachedBytes = new AtomicLong(0);
    private final AtomicInteger cachedNumEntries = new AtomicInteger(0);

    private CachedStreamEvents() {
      this.eventCache = CacheBuilder.newBuilder()
        .maximumSize(maxCachedEvents * 2)
        .expireAfterWrite(flushIntervalMs * 100, TimeUnit.MILLISECONDS)
        .removalListener(
          new RemovalListener<QueueName, BlockingQueue<QueueEntry>>() {
            @Override
            public void onRemoval(RemovalNotification<QueueName, BlockingQueue<QueueEntry>> notification) {
              try {
                flushQueues(
                  ImmutableMap.of(notification.getKey(),
                                  (TransactionAware) queueClientFactory.createProducer(notification.getKey())));
              } catch (IOException e) {
                LOG.error("Error while creating queue producer", e);
              }
            }
          }
        )
        .build(
          new CacheLoader<QueueName, BlockingQueue<QueueEntry>>() {
            @Override
            public BlockingQueue<QueueEntry> load(QueueName key) throws Exception {
              return new ArrayBlockingQueue<QueueEntry>(maxCachedEvents);
            }
          });
    }

    public ListenableFuture<Void> put(QueueName queueName, QueueEntry queueEntry)
      throws ExecutionException, InterruptedException {

      eventCache.get(queueName).put(queueEntry);

      long cachedSize = cachedBytes.addAndGet(queueEntry.getData().length);
      int cachedEntries = cachedNumEntries.incrementAndGet();
      if (cachedEntries >= maxCachedEvents || cachedSize >= maxCachedSizeBytes) {
        checkedFlush();
      }
      return future;
    }

    public void checkedFlush() {
      // Only one thread needs to run flush
      int cachedEntries = cachedNumEntries.get();
      if ((cachedEntries < maxCachedEvents && cachedBytes.get() < maxCachedSizeBytes)
        || !cachedNumEntries.compareAndSet(cachedEntries, 0)) {
        return;
      }
      cachedBytes.set(0);

      flush();
    }

    public void flush() {
      // Create producers
      Map<QueueName, TransactionAware> producerMap = Maps.newHashMap();
      try {
        for (Map.Entry<QueueName, BlockingQueue<QueueEntry>> entry : eventCache.asMap().entrySet()) {
          if (!entry.getValue().isEmpty()) {
            Queue2Producer producer = queueClientFactory.createProducer(entry.getKey());
            producerMap.put(entry.getKey(), (TransactionAware) producer);
          }
        }
      } catch (IOException e) {
        LOG.error("Error while creating queue producer during flush", e);
      }

      if (producerMap.isEmpty()) {
        return;
      }

      flushQueues(producerMap);
    }

    public void flushQueues(Map<QueueName, TransactionAware> producerMap) {
      try {
        TxManager txManager = new TxManager(opex, producerMap.values());
        try {
          txManager.start();

          // Enqueue
          for (Map.Entry<QueueName, TransactionAware> entry : producerMap.entrySet()) {
            BlockingQueue<QueueEntry> queue = eventCache.get(entry.getKey());
            if (queue.isEmpty()) {
              continue;
            }

            int size = queue.size();
            List<QueueEntry> entries = Lists.newArrayListWithCapacity(size);
            queue.drainTo(entries, size);
            if (entries.isEmpty()) {
              continue;
            }

            ((Queue2Producer) entry.getValue()).enqueue(entries);
          }

          // Commit
          txManager.commit();
          future.set(null);
        } catch (Exception e){
          LOG.error("Exception while committing stream events. Aborting transaction", e);
          future.setException(e);
          txManager.abort();
        }
      } catch (OperationException e) {
        LOG.error("Exception while aborting stream events transaction", e);
      }
    }
  }
}
