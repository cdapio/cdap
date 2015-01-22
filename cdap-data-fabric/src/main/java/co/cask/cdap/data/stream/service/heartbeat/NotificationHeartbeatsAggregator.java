/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data.stream.service.heartbeat;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.stream.notification.StreamSizeNotification;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamPropertyListener;
import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedNotFoundException;
import co.cask.cdap.notifications.service.NotificationContext;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link StreamsHeartbeatsAggregator} in which heartbeats are received as notifications. This implementation
 * uses the {@link NotificationService} to subscribe to heartbeats sent by Stream writers.
 */
public class NotificationHeartbeatsAggregator extends AbstractIdleService implements StreamsHeartbeatsAggregator {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationHeartbeatsAggregator.class);

  private static final int AGGREGATION_EXECUTOR_POOL_SIZE = 10;

  private final Map<String, Cancellable> streamHeartbeatsSubscriptions;
  private final NotificationService notificationService;
  private final StreamCoordinatorClient streamCoordinatorClient;

  private ListeningScheduledExecutorService scheduledExecutor;

  @Inject
  public NotificationHeartbeatsAggregator(NotificationService notificationService,
                                          StreamCoordinatorClient streamCoordinatorClient) {
    this.notificationService = notificationService;
    this.streamCoordinatorClient = streamCoordinatorClient;
    this.streamHeartbeatsSubscriptions = Maps.newHashMap();
  }

  @Override
  protected void startUp() throws Exception {
    this.scheduledExecutor = MoreExecutors.listeningDecorator(
      Executors.newScheduledThreadPool(AGGREGATION_EXECUTOR_POOL_SIZE,
                                       Threads.createDaemonThreadFactory("streams-heartbeats-aggregator")));
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      for (Cancellable subscription : streamHeartbeatsSubscriptions.values()) {
        subscription.cancel();
      }
    } finally {
      scheduledExecutor.shutdownNow();
    }
  }

  @Override
  public synchronized void aggregate(Set<String> streamNames) {
    Set<String> alreadyListeningStreams = Sets.newHashSet(streamHeartbeatsSubscriptions.keySet());
    for (final String streamName : streamNames) {
      if (alreadyListeningStreams.remove(streamName)) {
        continue;
      }
      try {
        aggregate(streamName);
      } catch (IOException e) {
        LOG.warn("Unable to listen to heartbeats of Stream {}", streamName, e);
      }
    }

    // Remove subscriptions to the heartbeats we used to listen to before the call to that method,
    // but don't anymore
    for (String outdatedStream : alreadyListeningStreams) {
      Cancellable cancellable = streamHeartbeatsSubscriptions.remove(outdatedStream);
      if (cancellable != null) {
        cancellable.cancel();
      }
    }
  }

  private synchronized void aggregate(String streamName) throws IOException {
    final Aggregator aggregator = new Aggregator(streamName);

    final Cancellable heartbeatsSubscription = subscribeToStreamHeartbeats(streamName, aggregator);
    final Cancellable truncationListener =
      streamCoordinatorClient.addListener(streamName, new StreamPropertyListener() {
        @Override
        public void generationChanged(String streamName, int generation) {
          aggregator.reset();
        }
      });

    // Schedule aggregation logic
    final ScheduledFuture<?> scheduledFuture = scheduledExecutor.scheduleAtFixedRate(
      aggregator, Constants.Notification.Stream.INIT_HEARTBEAT_AGGREGATION_DELAY,
      Constants.Notification.Stream.HEARTBEAT_AGGREGATION_DELAY, TimeUnit.SECONDS);

    streamHeartbeatsSubscriptions.put(streamName, new Cancellable() {
      @Override
      public void cancel() {
        truncationListener.cancel();
        heartbeatsSubscription.cancel();
        scheduledFuture.cancel(false);
      }
    });
  }

  /**
   * Subscribe to the notification feed concerning heartbeats of the feed {@code streamName}.
   *
   * @param streamName stream with heartbeats to subscribe to
   * @param aggregator heartbeats aggregator for the {@code streamName}
   */
  private Cancellable subscribeToStreamHeartbeats(String streamName, final Aggregator aggregator) throws IOException {
    try {
      final NotificationFeed heartbeatFeed = new NotificationFeed.Builder()
        .setNamespace("default")
        .setCategory(Constants.Notification.Stream.STREAM_HEARTBEAT_FEED_CATEGORY)
        .setName(Constants.Notification.Stream.STREAM_HEARTBEAT_FEED_NAME)
        .build();

      return notificationService.subscribe(
        heartbeatFeed, new NotificationHandler<StreamWriterHeartbeat>() {
          @Override
          public Type getNotificationFeedType() {
            return StreamWriterHeartbeat.class;
          }

          @Override
          public void received(StreamWriterHeartbeat heartbeat, NotificationContext notificationContext) {
            if (heartbeat.getType().equals(StreamWriterHeartbeat.Type.INIT)) {
              // Init heartbeats don't describe "new" data, hence we consider their data as part of the base count
              // If a writer is killed, a new one comes back up and sends an init heartbeat. The leader checks
              // if it already has a heartbeat from this writer and only adds the difference with the last heartbeat.
              StreamWriterHeartbeat lastHeartbeat = aggregator.getHeartbeats().get(heartbeat.getWriterID());
              long toAdd;
              if (lastHeartbeat == null) {
                toAdd = heartbeat.getAbsoluteDataSize();
              } else {
                // Recalibrate the leader's base count according to that particular writer's base count
                toAdd = heartbeat.getAbsoluteDataSize() - lastHeartbeat.getAbsoluteDataSize();
              }
              aggregator.getStreamBaseCount().addAndGet(toAdd);
            }
            aggregator.getHeartbeats().put(heartbeat.getWriterID(), heartbeat);
          }
        });
    } catch (NotificationFeedNotFoundException e) {
      throw new IOException(e);
    } catch (NotificationFeedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Runnable scheduled to aggregate the sizes of all stream writers. A notification is published
   * if the aggregated size is higher than a threshold.
   */
  private final class Aggregator implements Runnable {

    private final Map<Integer, StreamWriterHeartbeat> heartbeats;
    private final NotificationFeed streamFeed;
    private final AtomicLong streamBaseCount;

    // This boolean will ensure that an extra Stream notification is sent at CDAP start-up.
    private boolean initNotificationSent;

    protected Aggregator(String streamName) {
      this.heartbeats = Maps.newHashMap();
      this.streamBaseCount = new AtomicLong(0);
      this.streamFeed = new NotificationFeed.Builder()
        .setNamespace("default")
        .setCategory(Constants.Notification.Stream.STREAM_FEED_CATEGORY)
        .setName(streamName)
        .build();
      this.initNotificationSent = false;
    }

    public Map<Integer, StreamWriterHeartbeat> getHeartbeats() {
      return heartbeats;
    }

    public AtomicLong getStreamBaseCount() {
      return streamBaseCount;
    }

    public void reset() {
      heartbeats.clear();
      streamBaseCount.set(0);
    }

    @Override
    public void run() {
      int sum = 0;
      for (StreamWriterHeartbeat heartbeat : heartbeats.values()) {
        sum += heartbeat.getAbsoluteDataSize();
      }

      if (!initNotificationSent || sum - streamBaseCount.get() > Constants.Notification.Stream.DEFAULT_DATA_THRESHOLD) {
        try {
          initNotificationSent = true;
          publishNotification(sum);
        } finally {
          streamBaseCount.set(sum);
        }
      }
    }

    private void publishNotification(long absoluteSize) {
      try {
        notificationService.publish(
          streamFeed,
          new StreamSizeNotification(System.currentTimeMillis(), absoluteSize))
          .get();
      } catch (NotificationFeedException e) {
        LOG.warn("Error with notification feed {}", streamFeed, e);
      } catch (Throwable t) {
        LOG.warn("Could not publish notification on feed {}", streamFeed.getId(), t);
      }
    }

  }
}
