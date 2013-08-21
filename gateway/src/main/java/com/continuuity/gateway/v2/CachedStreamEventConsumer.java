package com.continuuity.gateway.v2;

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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class caches stream events and enqueues them in batch.
 */
public class CachedStreamEventConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(CachedStreamEventConsumer.class);

  private final OperationExecutor opex;
  private final QueueClientFactory queueClientFactory;

  private final Timer timer;

  private final StreamEventCodec serializer = new StreamEventCodec();

  private final AtomicReference<CachedStreamEvents> cachedStreamEventsRef =
    new AtomicReference<CachedStreamEvents>(new CachedStreamEvents());

  private final int maxCachedEvents;
  private final long maxCachedSizeBytes;
  private final long flushIntervalMs;

  @Inject
  public CachedStreamEventConsumer(CConfiguration cConfig, OperationExecutor opex,
                                   QueueClientFactory queueClientFactory) {
    this.opex = opex;
    this.queueClientFactory = queueClientFactory;
    this.timer = new Timer(String.format("%s-thread", getClass().getCanonicalName()), true);

    maxCachedEvents = cConfig.getInt(GatewayConstants.ConfigKeys.MAX_CACHED_STREAM_EVENTS_NUM,
                                     GatewayConstants.DEFAULT_MAX_CACHED_STREAM_EVENTS_NUM);
    maxCachedSizeBytes = cConfig.getLong(GatewayConstants.ConfigKeys.MAX_CACHED_STREAM_EVENTS_BYTES,
                                         GatewayConstants.DEFAULT_MAX_CACHED_STREAM_EVENTS_BYTES);
    flushIntervalMs = cConfig.getLong(GatewayConstants.ConfigKeys.STREAM_EVENTS_FLUSH_INTERVAL_MS,
                                      GatewayConstants.DEFAULT_STREAM_EVENTS_FLUSH_INTERVAL_MS);
  }

  public void start() {
    timer.scheduleAtFixedRate(
      new TimerTask() {
        @Override
        public void run() {
          flush();
        }
      },
      flushIntervalMs, flushIntervalMs
    );
  }

  public void stop() {
    // flush
    flush();
    timer.cancel();
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
    return cachedStreamEventsRef.get().put(queueName, new QueueEntry(bytes));
  }

  private void flush() {
    try {
      // Atomically replace the CachedStreamEvents
      CachedStreamEvents cachedStreamEvents = cachedStreamEventsRef.getAndSet(new CachedStreamEvents());

      // Enqueue
      ListMultimap<QueueName, QueueEntry> eventsMap = cachedStreamEvents.prepareForFlush();
      Map<QueueName, TransactionAware> producerMap = Maps.newHashMap();
      for (Map.Entry<QueueName, Collection<QueueEntry>> entry : eventsMap.asMap().entrySet()) {
        Queue2Producer producer = queueClientFactory.createProducer(entry.getKey());
        producerMap.put(entry.getKey(), (TransactionAware) producer);
      }

      TxManager txManager = new TxManager(opex, producerMap.values());
      try {
        txManager.start();

        for (Map.Entry<QueueName, Collection<QueueEntry>> entry : eventsMap.asMap().entrySet()) {
          ((Queue2Producer) producerMap.get(entry.getKey())).enqueue(entry.getValue());
        }

        // Commit
        txManager.commit();
        cachedStreamEvents.getFuture().set(null);
      } catch (Exception e){
        LOG.error("Exception while committing stream events. Aborting transaction", e);
        cachedStreamEvents.getFuture().setException(e);
        txManager.abort();
      }
    } catch (Exception e) {
      LOG.error("Exception while aborting stream events transaction", e);
    }
  }

  private final class CachedStreamEvents {
    private final ListMultimap<QueueName, QueueEntry> streamEventsMap =
      Multimaps.synchronizedListMultimap(ArrayListMultimap.<QueueName, QueueEntry>create());

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final SettableFuture<Void> future = SettableFuture.create();
    private final AtomicLong cachedBytes = new AtomicLong(0);
    private final AtomicBoolean stop = new AtomicBoolean(false);

    public ListenableFuture<Void> put(QueueName queueUri, QueueEntry queueEntry) {
      try {
        lock.readLock().lock();

        if (stop.get()) {
          throw new IllegalStateException("Cannot add new stream events when ready for flush");
        }

        streamEventsMap.put(queueUri, queueEntry);
      } finally {
        lock.readLock().unlock();
      }

      long cachedSize = cachedBytes.addAndGet(queueEntry.getData().length);
      if (streamEventsMap.size() >= maxCachedEvents || cachedSize >= maxCachedSizeBytes) {
        flush();
      }
      return future;
    }

    public ListMultimap<QueueName, QueueEntry> prepareForFlush() {
      try {
        lock.writeLock().lock();
        stop.set(true);
      } finally {
        lock.writeLock().unlock();
      }
      return streamEventsMap;
    }

    public SettableFuture<Void> getFuture() {
      if (!stop.get()) {
        throw new IllegalStateException("Writable future cannot be accessed until prepareForFlush is called");
      }
      return future;
    }
  }
}
