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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.stream.notification.StreamSizeNotification;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamPropertyListener;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.service.NotificationService;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stream service running in local mode.
 */
public class LocalStreamService extends AbstractStreamService {
  private static final Logger LOG = LoggerFactory.getLogger(LocalStreamService.class);

  private final NotificationService notificationService;
  private final StreamAdmin streamAdmin;
  private final StreamWriterSizeCollector streamWriterSizeCollector;
  private final StreamMetaStore streamMetaStore;
  private final ConcurrentMap<String, StreamSizeAggregator> aggregators;
  private boolean isInit;

  @Inject
  public LocalStreamService(StreamCoordinatorClient streamCoordinatorClient,
                            StreamFileJanitorService janitorService,
                            StreamMetaStore streamMetaStore,
                            StreamAdmin streamAdmin,
                            StreamWriterSizeCollector streamWriterSizeCollector,
                            NotificationService notificationService) {
    super(streamCoordinatorClient, janitorService, streamWriterSizeCollector);
    this.streamAdmin = streamAdmin;
    this.streamMetaStore = streamMetaStore;
    this.streamWriterSizeCollector = streamWriterSizeCollector;
    this.notificationService = notificationService;
    this.aggregators = Maps.newConcurrentMap();
    this.isInit = true;
  }

  @Override
  protected void initialize() throws Exception {
    for (StreamSpecification streamSpec : streamMetaStore.listStreams(Constants.DEFAULT_NAMESPACE)) {
      StreamConfig config = streamAdmin.getConfig(streamSpec.getName());
      long filesSize = StreamUtils.fetchStreamFilesSize(config);
      createSizeAggregator(streamSpec.getName(), filesSize, config.getNotificationThresholdMB());
    }
  }

  @Override
  protected void doShutdown() throws Exception {
    for (StreamSizeAggregator streamSizeAggregator : aggregators.values()) {
      streamSizeAggregator.cancel();
    }
  }

  @Override
  protected void runOneIteration() throws Exception {
    // Get stream size - which will be the entire size - and send a notification if the size is big enough
    for (StreamSpecification streamSpec : streamMetaStore.listStreams(Constants.DEFAULT_NAMESPACE)) {
      StreamSizeAggregator streamSizeAggregator = aggregators.get(streamSpec.getName());
      if (streamSizeAggregator == null) {
        // First time that we see this Stream here
        StreamConfig config = streamAdmin.getConfig(streamSpec.getName());
        streamSizeAggregator = createSizeAggregator(streamSpec.getName(), 0, config.getNotificationThresholdMB());
      }
      streamSizeAggregator.checkAggregatedSize();
    }
    isInit = false;
  }

  /**
   * Create a new aggregator for the {@code streamName}, and add it to the existing map of {@link Cancellable}
   * {@code aggregators}. This method does not cancel previously existing aggregator associated to the
   * {@code streamName}.
   *
   * @param streamName stream name to create a new aggregator for
   * @param baseCount stream size from which to start aggregating
   * @return the created {@link StreamSizeAggregator}
   */
  private StreamSizeAggregator createSizeAggregator(String streamName, long baseCount, int threshold) {

    // Handle threshold changes
    final Cancellable thresholdSubscription =
      getStreamCoordinatorClient().addListener(streamName, new StreamPropertyListener() {
        @Override
        public void thresholdChanged(String streamName, int threshold) {
          StreamSizeAggregator aggregator = aggregators.get(streamName);
          while (aggregator == null) {
            Thread.yield();
            aggregator = aggregators.get(streamName);
          }
          aggregator.setStreamThresholdMB(threshold);
        }
      });

    // Handle stream truncation, by creating creating a new empty aggregator for the stream
    // and cancelling the existing one
    final Cancellable truncationSubscription =
      getStreamCoordinatorClient().addListener(streamName, new StreamPropertyListener() {
        @Override
        public void generationChanged(String streamName, int generation) {
          StreamSizeAggregator aggregator = aggregators.get(streamName);
          while (aggregator == null) {
            Thread.yield();
            aggregator = aggregators.get(streamName);
          }
          aggregator.resetCount();
        }
      });

    StreamSizeAggregator newAggregator = new StreamSizeAggregator(streamName, baseCount, threshold, new Cancellable() {
      @Override
      public void cancel() {
        thresholdSubscription.cancel();
        truncationSubscription.cancel();
      }
    });
    aggregators.put(streamName, newAggregator);
    return newAggregator;
  }

  /**
   * Aggregate the sizes of all stream writers. A notification is published if the aggregated
   * size is higher than a threshold.
   */
  private final class StreamSizeAggregator implements Cancellable {
    private final AtomicLong streamInitSize;
    private final NotificationFeed streamFeed;
    private final String streamName;
    private final AtomicLong streamBaseCount;
    private final AtomicInteger streamThresholdMB;
    private final Cancellable cancellable;

    protected StreamSizeAggregator(String streamName, long baseCount, int streamThresholdMB, Cancellable cancellable) {
      this.streamName = streamName;
      this.streamInitSize = new AtomicLong(baseCount);
      this.streamBaseCount = new AtomicLong(baseCount);
      this.cancellable = cancellable;
      this.streamFeed = new NotificationFeed.Builder()
        .setNamespace(Constants.DEFAULT_NAMESPACE)
        .setCategory(Constants.Notification.Stream.STREAM_FEED_CATEGORY)
        .setName(streamName)
        .build();
      this.streamThresholdMB = new AtomicInteger(streamThresholdMB);
    }

    @Override
    public void cancel() {
      cancellable.cancel();
    }

    /**
     * Reset the counts of this {@link StreamSizeAggregator} to zero.
     */
    public void resetCount() {
      streamBaseCount.set(0);
      streamInitSize.set(0);
    }

    /**
     * Set the notification threshold for the stream that this {@link StreamSizeAggregator} is linked to.
     *
     * @param newThreshold new notification threshold, in megabytes
     */
    public void setStreamThresholdMB(int newThreshold) {
      streamThresholdMB.set(newThreshold);
    }

    /**
     * Check that the aggregated size of the heartbeats received by all Stream writers is higher than some threshold.
     * If it is, we publish a notification.
     */
    public void checkAggregatedSize() {
      long sum = streamInitSize.get() + streamWriterSizeCollector.getTotalCollected(streamName);
      if (isInit || sum - streamBaseCount.get() > toBytes(streamThresholdMB.get())) {
        try {
          publishNotification(sum);
        } finally {
          streamBaseCount.set(sum);
        }
      }
    }

    private long toBytes(int mb) {
      return ((long) mb) * 1024 * 1024;
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
