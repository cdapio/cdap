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
import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.service.NotificationContext;
import co.cask.cdap.notifications.service.NotificationException;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class NotificationHeartbeatsAggregator2 extends AbstractScheduledService implements StreamsHeartbeatsAggregator {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationHeartbeatsAggregator2.class);

  private final NotificationService notificationService;
  private final Map<String, Map<Integer, StreamWriterHeartbeat.StreamSize>> streamsSizes;
  private Cancellable heartbeatsSubscription;

  public NotificationHeartbeatsAggregator2(NotificationService notificationService) {
    this.notificationService = notificationService;
    this.streamsSizes = Maps.newConcurrentMap();
  }

  @Override
  protected void startUp() throws Exception {
    heartbeatsSubscription = subscribeToHeartbeatsFeed();
  }

  @Override
  protected void shutDown() throws Exception {
    if (heartbeatsSubscription != null) {
      heartbeatsSubscription.cancel();
    }
  }

  @Override
  protected void runOneIteration() throws Exception {
    for(Map.Entry<String, Map<Integer, StreamWriterHeartbeat.StreamSize>> entry : streamsSizes.entrySet()) {
      String streamName = entry.getKey();
      int sum = 0;
      for (StreamWriterHeartbeat.StreamSize size : entry.getValue().values()) {
        sum += size.getAbsoluteDataSize();
      }

      if (!initNotificationSent || sum - streamBaseCount.get() > Constants.Notification.Stream.DEFAULT_DATA_THRESHOLD) {
        try {
          initNotificationSent = true;
          publishNotification(streamName, sum);
        } finally {
          streamBaseCount.set(sum);
        }
      }
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(Constants.Notification.Stream.INIT_HEARTBEAT_AGGREGATION_DELAY,
                                          Constants.Notification.Stream.HEARTBEAT_AGGREGATION_DELAY,
                                          TimeUnit.SECONDS);
  }

  @Override
  public void aggregate(Set<String> streamNames) {
    Set<String> alreadyListeningStreams = Sets.newHashSet(streamsSizes.keySet());
    for (String streamName : streamNames) {
      if (alreadyListeningStreams.remove(streamName)) {
        continue;
      }

      streamsSizes.put(streamName, Maps.<Integer, StreamWriterHeartbeat.StreamSize>newHashMap());
    }

    // Remove subscriptions to the heartbeats we used to listen to before the call to that method,
    // but don't anymore
    for (String outdatedStream : alreadyListeningStreams) {
      streamsSizes.remove(outdatedStream);
    }
  }

  private Cancellable subscribeToHeartbeatsFeed() throws NotificationException {
    final NotificationFeed heartbeatsFeed = new NotificationFeed.Builder()
      .setNamespace("default")
      .setCategory(Constants.Notification.Stream.STREAM_HEARTBEAT_FEED_CATEGORY)
      .setName(Constants.Notification.Stream.STREAM_HEARTBEAT_FEED_NAME)
      .build();
    return notificationService.subscribe(heartbeatsFeed, new NotificationHandler<StreamWriterHeartbeat>() {
      @Override
      public Type getNotificationFeedType() {
        return StreamWriterHeartbeat.class;
      }

      @Override
      public void received(StreamWriterHeartbeat notification, NotificationContext notificationContext) {
        // TODO add heartbeat to map
      }
    });
  }

  private void publishNotification(String streamName, long absoluteSize) {
    NotificationFeed feed = new NotificationFeed.Builder()
      .setNamespace("default")
      .setCategory(Constants.Notification.Stream.STREAM_FEED_CATEGORY)
      .setName(streamName)
      .build();
    try {
      notificationService.publish( feed, new StreamSizeNotification(System.currentTimeMillis(), absoluteSize)).get();
    } catch (NotificationFeedException e) {
      LOG.warn("Error with notification feed {}", feed, e);
    } catch (Throwable t) {
      LOG.warn("Could not publish notification for stream {}", streamName, t);
    }
  }

  /**
   * Runnable scheduled to aggregate the sizes of all stream writers. A notification is published
   * if the aggregated size is higher than a threshold.
   */
  private final class Aggregator implements Runnable {

    private final Map<Integer, StreamWriterHeartbeat.StreamSize> heartbeats;
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

    public Map<Integer, StreamWriterHeartbeat.StreamSize> getHeartbeats() {
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
      for (StreamWriterHeartbeat.StreamSize heartbeat : heartbeats.values()) {
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
