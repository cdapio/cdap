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
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.service.NotificationContext;
import co.cask.cdap.notifications.service.NotificationException;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
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
 * {@link StreamsHeartbeatsAggregator} of notification heartbeats.
 */
public class NotificationHeartbeatsAggregator extends AbstractScheduledService implements StreamsHeartbeatsAggregator {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationHeartbeatsAggregator.class);

  private final StreamAdmin streamAdmin;
  private final NotificationService notificationService;
  private final StreamCoordinatorClient streamCoordinatorClient;
  private final Map<String, Aggregator> aggregators;
  private Cancellable heartbeatsSubscription;

  @Inject
  public NotificationHeartbeatsAggregator(StreamAdmin streamAdmin, NotificationService notificationService,
                                          StreamCoordinatorClient streamCoordinatorClient) {
    this.streamAdmin = streamAdmin;
    this.notificationService = notificationService;
    this.streamCoordinatorClient = streamCoordinatorClient;
    this.aggregators = Maps.newConcurrentMap();
  }

  @Override
  protected void startUp() throws Exception {
    heartbeatsSubscription = subscribeToHeartbeatsFeed();
  }

  @Override
  protected void shutDown() throws Exception {
    for (Aggregator aggregator : aggregators.values()) {
      if (aggregator != null) {
        aggregator.cancel();
      }
    }

    if (heartbeatsSubscription != null) {
      heartbeatsSubscription.cancel();
    }
  }

  @Override
  protected void runOneIteration() throws Exception {
    for (Aggregator aggregator : aggregators.values()) {
      aggregator.checkAggregatedSize();
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
    Set<String> existingAggregators = Sets.newHashSet(aggregators.keySet());
    for (String streamName : streamNames) {
      if (existingAggregators.remove(streamName)) {
        continue;
      }

      long filesSize = 0;
      try {
        StreamConfig config = streamAdmin.getConfig(streamName);
        filesSize = StreamUtils.fetchStreamFilesSize(config);
      } catch (IOException e) {
        LOG.error("Could not compute sizes of files for stream {}", streamName);
      }

      Aggregator aggregator = new Aggregator(streamName, filesSize);
      aggregators.put(streamName, aggregator);
    }

    // Remove subscriptions to the heartbeats we used to listen to before the call to that method,
    // but don't anymore
    for (String outdatedStream : existingAggregators) {
      Aggregator aggregator = aggregators.remove(outdatedStream);
      if (aggregator != null) {
        aggregator.cancel();
      }
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
      public void received(StreamWriterHeartbeat heartbeat, NotificationContext notificationContext) {
        for (Map.Entry<String, Long> entry : heartbeat.getStreamsSizes().entrySet()) {
          Aggregator aggregator = aggregators.get(entry.getKey());
          if (aggregator == null) {
            continue;
          }
          aggregator.bytesReceived(heartbeat.getInstanceId(), entry.getValue());
        }
      }
    });
  }

  /**
   * Runnable scheduled to aggregate the sizes of all stream writers. A notification is published
   * if the aggregated size is higher than a threshold.
   */
  private final class Aggregator implements Cancellable {

    private final Map<Integer, Long> streamWriterSizes;
    private final NotificationFeed streamFeed;
    private final AtomicLong streamBaseCount;
    private final Cancellable truncationSubscription;

    // This boolean will ensure that an extra Stream notification is sent at CDAP start-up.
    private boolean initNotificationSent;

    protected Aggregator(String streamName, long baseCount) {
      this.streamWriterSizes = Maps.newHashMap();
      this.streamBaseCount = new AtomicLong(baseCount);
      this.streamFeed = new NotificationFeed.Builder()
        .setNamespace("default")
        .setCategory(Constants.Notification.Stream.STREAM_FEED_CATEGORY)
        .setName(streamName)
        .build();
      this.initNotificationSent = false;

      this.truncationSubscription = streamCoordinatorClient.addListener(streamName, new StreamPropertyListener() {
        @Override
        public void generationChanged(String streamName, int generation) {
          reset();
        }
      });
    }

    @Override
    public void cancel() {
      truncationSubscription.cancel();
    }

    private void reset() {
      streamWriterSizes.clear();
      streamBaseCount.set(0);
    }

    /**
     * Notify this aggregator that a certain number of bytes have been received from the stream writer with instance
     * {@code instanceId}.
     *
     * @param instanceId id of the stream writer from which we received some bytes
     * @param nbBytes number of bytes of data received
     */
    public void bytesReceived(int instanceId, long nbBytes) {
      Long lastSize = streamWriterSizes.get(instanceId);
      if (lastSize == null) {
        streamWriterSizes.put(instanceId, nbBytes);
        return;
      }
      streamWriterSizes.put(instanceId, lastSize + nbBytes);
    }

    /**
     * Check that the aggregated size of the heartbeats received by all Stream writers is higher than some threshold.
     * If it is, we publish a notification.
     */
    public void checkAggregatedSize() {
      int sum = 0;
      for (Long size : streamWriterSizes.values()) {
        sum += size;
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
        notificationService.publish(streamFeed, new StreamSizeNotification(System.currentTimeMillis(), absoluteSize))
          .get();
      } catch (NotificationFeedException e) {
        LOG.warn("Error with notification feed {}", streamFeed, e);
      } catch (Throwable t) {
        LOG.warn("Could not publish notification on feed {}", streamFeed.getId(), t);
      }
    }
  }
}
