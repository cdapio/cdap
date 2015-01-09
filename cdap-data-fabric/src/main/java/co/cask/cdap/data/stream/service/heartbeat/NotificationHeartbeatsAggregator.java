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
import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedNotFoundException;
import co.cask.cdap.notifications.service.NotificationContext;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link StreamsHeartbeatsAggregator} in which heartbeats are received as notifications. This implementation
 * uses the {@link NotificationService} to subscribe to heartbeats sent by Stream writers.
 */
// TODO this guy should have a way to get the Threshold for the streams it is doing aggregation
public class NotificationHeartbeatsAggregator implements StreamsHeartbeatsAggregator {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationHeartbeatsAggregator.class);

  private static final int AGGREGATION_EXECUTOR_POOL_SIZE = 10;

  private final Map<String, Cancellable> streamHeartbeatsSubscriptions;
  private final NotificationService notificationService;
  private final ListeningScheduledExecutorService scheduledExecutor;

  @Inject
  public NotificationHeartbeatsAggregator(NotificationService notificationService) {
    this.notificationService = notificationService;
    this.streamHeartbeatsSubscriptions = Maps.newHashMap();
    this.scheduledExecutor = MoreExecutors.listeningDecorator(
      Executors.newScheduledThreadPool(AGGREGATION_EXECUTOR_POOL_SIZE,
                                       Threads.createDaemonThreadFactory("streams-heartbeats-aggregator")));
  }

  @Override
  public synchronized void listenToStreams(Collection<String> streamNames) {
    Set<String> alreadyListeningStreams = streamHeartbeatsSubscriptions.keySet();
    for (final String streamName : streamNames) {
      if (alreadyListeningStreams.remove(streamName)) {
        continue;
      }

      // Keep track of the heartbeats received for the current stream
      final Map<Integer, StreamWriterHeartbeat> heartbeats = Maps.newHashMap();
      final AtomicInteger baseCount = new AtomicInteger(0);

      final NotificationFeed streamFeed = new NotificationFeed.Builder()
        .setNamespace("default")
        .setCategory(Constants.Notification.Stream.STREAM_FEED_CATEGORY)
        .setName(streamName)
        .build();

      subscribeToStreamHeartbeats(streamName, heartbeats, baseCount);


      // Schedule aggregation logic
      scheduledExecutor.schedule(new Aggregator(heartbeats, streamFeed, baseCount), 1, TimeUnit.SECONDS);
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

  /**
   * Subscribe to the notification feed concerning heartbeats of the feed {@code streamName}.
   *
   * @param streamName stream with heartbeats to subscribe to.
   * @param heartbeats map to store the last heartbeat received per Stream writer.
   * @param baseCount base aggregation count.
   */
  private void subscribeToStreamHeartbeats(String streamName, final Map<Integer, StreamWriterHeartbeat> heartbeats,
                                           final AtomicInteger baseCount) {
    try {
      final NotificationFeed heartbeatFeed = new NotificationFeed.Builder()
        .setNamespace("default")
        .setCategory(Constants.Notification.Stream.STREAM_HEARTBEAT_FEED_CATEGORY)
        .setName(streamName)
        .build();

      Cancellable subscription = notificationService.subscribe(
        heartbeatFeed, new NotificationHandler<StreamWriterHeartbeat>() {
          @Override
          public Type getNotificationFeedType() {
            return StreamWriterHeartbeat.class;
          }

          @Override
          public void received(StreamWriterHeartbeat heartbeat, NotificationContext notificationContext) {
            if (heartbeat.getType().equals(StreamWriterHeartbeat.Type.INIT)) {
              // Init heartbeats don't describe "new" data, hence we consider
              // their data as part of the base count
              baseCount.addAndGet(heartbeat.getAbsoluteDataSize());
            }
            heartbeats.put(heartbeat.getWriterID(), heartbeat);
          }
        });
      streamHeartbeatsSubscriptions.put(streamName, subscription);
    } catch (NotificationFeedNotFoundException e) {
      LOG.error("Heartbeat feed for Stream {} does not exist", streamName);
    } catch (NotificationFeedException e) {
      LOG.warn("Could not subscribe to heartbeat feed for Stream {}", streamName);
    }
  }

  /**
   * Runnable scheduled to aggregate the sizes of all stream writers. A notification is published
   * if the aggregated size is higher than a threshold.
   */
  private final class Aggregator implements Runnable {

    private final Map<Integer, StreamWriterHeartbeat> heartbeats;
    private final NotificationFeed streamFeed;
    private final AtomicInteger streamBaseCount;

    protected Aggregator(Map<Integer, StreamWriterHeartbeat> heartbeats, NotificationFeed streamFeed,
                         AtomicInteger streamBaseCount) {
      this.heartbeats = heartbeats;
      this.streamFeed = streamFeed;
      this.streamBaseCount = streamBaseCount;
    }

    @Override
    public void run() {
      // For now just count the last heartbeat present in the map, but we should set a sort of sliding window for
      // the aggregator, and it would look for the last heartbeat in that window only
      // TODO why should we remember more than the last heartbeat?
      int sum = 0;
      for (StreamWriterHeartbeat heartbeat : heartbeats.values()) {
        sum += heartbeat.getAbsoluteDataSize();
      }

      if (sum - streamBaseCount.get() > Constants.Notification.Stream.DEFAULT_DATA_THRESHOLD) {
        try {
          publishNotification(sum);
        } finally {
          streamBaseCount.set(sum);
        }
      }
      scheduledExecutor.schedule(new Aggregator(heartbeats, streamFeed, streamBaseCount), 5, TimeUnit.SECONDS);
    }

    private void publishNotification(int updatedCount) {
      try {
        // TODO thing about the kind of notification to send here
        notificationService.publish(streamFeed, String.format("Has received %dMB of data total", updatedCount)).get();
      } catch (NotificationFeedException e) {
        LOG.warn("Error with notification feed {}", streamFeed, e);
      } catch (Throwable t) {
        LOG.warn("Could not publish notification on feed {}", streamFeed.getId(), t);
      }
    }

  }
}
