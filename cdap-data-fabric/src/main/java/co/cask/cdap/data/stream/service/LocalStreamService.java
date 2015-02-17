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
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.Id;
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
  private final ConcurrentMap<Id.Stream, StreamSizeAggregator> aggregators;
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
    Id.Namespace namespace = Id.Namespace.from(Constants.DEFAULT_NAMESPACE);
    //TODO: use streamMetaStore.listStreams() instead
    for (StreamSpecification streamSpec : streamMetaStore.listStreams(namespace)) {
      Id.Stream streamId = Id.Stream.from(namespace, streamSpec.getName());
      StreamConfig config = streamAdmin.getConfig(streamId);
      long filesSize = StreamUtils.fetchStreamFilesSize(config);
      createSizeAggregator(streamId, filesSize, config.getNotificationThresholdMB());
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
    Id.Namespace namespace = Id.Namespace.from(Constants.DEFAULT_NAMESPACE);
    // Get stream size - which will be the entire size - and send a notification if the size is big enough
    //TODO: use streamMetaStore.listStreams() instead
    for (StreamSpecification streamSpec : streamMetaStore.listStreams(namespace)) {
      Id.Stream streamId = Id.Stream.from(namespace, streamSpec.getName());
      StreamSizeAggregator streamSizeAggregator = aggregators.get(streamId);
      if (streamSizeAggregator == null) {
        // First time that we see this Stream here
        StreamConfig config = streamAdmin.getConfig(streamId);
        streamSizeAggregator = createSizeAggregator(streamId, 0, config.getNotificationThresholdMB());
      }
      streamSizeAggregator.checkAggregatedSize();
    }
    isInit = false;
  }

  /**
   * Create a new aggregator for the {@code streamId}, and add it to the existing map of {@link Cancellable}
   * {@code aggregators}. This method does not cancel previously existing aggregator associated to the
   * {@code streamId}.
   *
   * @param streamId stream name to create a new aggregator for
   * @param baseCount stream size from which to start aggregating
   * @return the created {@link StreamSizeAggregator}
   */
  private StreamSizeAggregator createSizeAggregator(Id.Stream streamId, long baseCount, int threshold) {

    // Handle threshold changes
    final Cancellable thresholdSubscription =
      getStreamCoordinatorClient().addListener(streamId, new StreamPropertyListener() {
        @Override
        public void thresholdChanged(Id.Stream streamId, int threshold) {
          StreamSizeAggregator aggregator = aggregators.get(streamId);
          while (aggregator == null) {
            Thread.yield();
            aggregator = aggregators.get(streamId);
          }
          aggregator.setStreamThresholdMB(threshold);
        }
      });

    // Handle stream truncation, by creating creating a new empty aggregator for the stream
    // and cancelling the existing one
    final Cancellable truncationSubscription =
      getStreamCoordinatorClient().addListener(streamId, new StreamPropertyListener() {
        @Override
        public void generationChanged(Id.Stream streamId, int generation) {
          StreamSizeAggregator aggregator = aggregators.get(streamId);
          while (aggregator == null) {
            Thread.yield();
            aggregator = aggregators.get(streamId);
          }
          aggregator.resetCount();
        }
      });

    StreamSizeAggregator newAggregator = new StreamSizeAggregator(streamId, baseCount, threshold, new Cancellable() {
      @Override
      public void cancel() {
        thresholdSubscription.cancel();
        truncationSubscription.cancel();
      }
    });
    aggregators.put(streamId, newAggregator);
    return newAggregator;
  }

  /**
   * Aggregate the sizes of all stream writers. A notification is published if the aggregated
   * size is higher than a threshold.
   */
  private final class StreamSizeAggregator implements Cancellable {
    private final AtomicLong streamInitSize;
    private final Id.NotificationFeed streamFeed;
    private final Id.Stream streamId;
    private final AtomicLong streamBaseCount;
    private final AtomicInteger streamThresholdMB;
    private final Cancellable cancellable;

    protected StreamSizeAggregator(Id.Stream streamId, long baseCount, int streamThresholdMB, Cancellable cancellable) {
      this.streamId = streamId;
      this.streamInitSize = new AtomicLong(baseCount);
      this.streamBaseCount = new AtomicLong(baseCount);
      this.cancellable = cancellable;
      this.streamFeed = new Id.NotificationFeed.Builder()
        .setNamespaceId(streamId.getNamespaceId())
        .setCategory(Constants.Notification.Stream.STREAM_FEED_CATEGORY)
        .setName(streamId.getName())
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
      long sum = streamInitSize.get() + streamWriterSizeCollector.getTotalCollected(streamId);
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
