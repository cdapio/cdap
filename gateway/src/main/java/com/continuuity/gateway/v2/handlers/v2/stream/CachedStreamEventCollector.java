package com.continuuity.gateway.v2.handlers.v2.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.queue.QueueEntry;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.streamevent.StreamEventCodec;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
  public CachedStreamEventCollector(CConfiguration cConfig, TransactionSystemClient txClient,
                                    QueueClientFactory queueClientFactory) {
    this.flushTimer = new Timer("stream-rest-flush-thread", true);

    int maxCachedEventsPerStream = cConfig.getInt(Constants.Gateway.MAX_CACHED_EVENTS_PER_STREAM_NUM,
                                                  Constants.Gateway.DEFAULT_MAX_CACHED_EVENTS_PER_STREAM_NUM);
    int maxCachedEvents = cConfig.getInt(Constants.Gateway.MAX_CACHED_STREAM_EVENTS_NUM,
                                         Constants.Gateway.DEFAULT_MAX_CACHED_STREAM_EVENTS_NUM);
    long maxCachedSizeBytes = cConfig.getLong(Constants.Gateway.MAX_CACHED_STREAM_EVENTS_BYTES,
                                              Constants.Gateway.DEFAULT_MAX_CACHED_STREAM_EVENTS_BYTES);
    this.flushIntervalMs = cConfig.getLong(Constants.Gateway.STREAM_EVENTS_FLUSH_INTERVAL_MS,
                                           Constants.Gateway.DEFAULT_STREAM_EVENTS_FLUSH_INTERVAL_MS);

    int numThreads = cConfig.getInt(Constants.Gateway.STREAM_EVENTS_CALLBACK_NUM_THREADS,
                                    Constants.Gateway.DEFAULT_STREAM_EVENTS_CALLBACK_NUM_THREADS);
    this.callbackExecutorService = Executors.newFixedThreadPool(numThreads,
                                                                new ThreadFactoryBuilder()
                                                                  .setDaemon(true)
                                                                  .setNameFormat("stream-rest-callback-thread")
                                                                  .build()
    );

    this.cachedStreamEvents = new CachedStreamEvents(txClient, queueClientFactory, callbackExecutorService,
                                                     maxCachedSizeBytes, maxCachedEvents,
                                                     maxCachedEventsPerStream);
  }

  // Optional injection of MetricsCollectionService
  @Inject(optional = true)
  void setMetricsCollectionService(@Nullable MetricsCollectionService metricsCollectionService) {
    cachedStreamEvents.setMetricsCollectionService(metricsCollectionService);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting up {}", this.getClass().getSimpleName());
    flushTimer.scheduleAtFixedRate(
      new TimerTask() {
        @Override
        public void run() {
          LOG.trace("Running flush from timer task.");
          cachedStreamEvents.flush(false);
          LOG.trace("Done running flush from timer task.");
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
   */
  public void collect(StreamEvent event, String accountId, FutureCallback<Void> callback) {
    try {
      byte[] bytes = serializer.encodePayload(event);
      Preconditions.checkArgument(bytes != null, String.format("Could not serialize event: %s", event));

      String destination = event.getHeaders().get(Constants.Gateway.HEADER_DESTINATION_STREAM);
      if (destination == null) {
        LOG.trace("Enqueuing an event that has no destination. Using 'default' instead.");
        destination = "default";
      }

      QueueName queueName = QueueName.fromStream(destination);
      cachedStreamEvents.put(queueName, new QueueEntry(bytes), callback);
    } catch (Throwable e) {
      callback.onFailure(e);
    }
  }
}
