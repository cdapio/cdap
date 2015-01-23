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

import java.util.Map;
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
  private final Map<String, Aggregator> aggregators;
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
    for (StreamSpecification streamSpec : streamMetaStore.listStreams()) {
      StreamConfig config = streamAdmin.getConfig(streamSpec.getName());
      long filesSize = StreamUtils.fetchStreamFilesSize(config);
      aggregators.put(streamSpec.getName(), new Aggregator(streamSpec.getName(), filesSize));
    }
  }

  @Override
  protected void doShutdown() throws Exception {
    for (Aggregator aggregator : aggregators.values()) {
      aggregator.cancel();
    }
  }

  @Override
  protected void runOneIteration() throws Exception {
    // Get stream size - which will be the entire size - and send a notification if the size is big enough
    for (StreamSpecification streamSpec : streamMetaStore.listStreams()) {
      Aggregator aggregator = aggregators.get(streamSpec.getName());
      if (aggregator == null) {
        // First time that we see this Stream here
        aggregator = new Aggregator(streamSpec.getName(), 0);
        aggregators.put(streamSpec.getName(), aggregator);
      }
      aggregator.checkAggregatedSize();
    }
    isInit = false;
  }

  /**
   * Aggregate the sizes of all stream writers. A notification is published if the aggregated
   * size is higher than a threshold.
   */
  private final class Aggregator implements Cancellable {
    private final AtomicLong streamInitSize;
    private final NotificationFeed streamFeed;
    private final String streamName;
    private final AtomicLong streamBaseCount;
    private final Cancellable truncationSubscription;

    protected Aggregator(String streamName, long baseCount) {
      this.streamName = streamName;
      this.streamInitSize = new AtomicLong(baseCount);
      this.streamBaseCount = new AtomicLong(baseCount);
      this.streamFeed = new NotificationFeed.Builder()
        .setNamespace(Constants.DEFAULT_NAMESPACE)
        .setCategory(Constants.Notification.Stream.STREAM_FEED_CATEGORY)
        .setName(streamName)
        .build();

      this.truncationSubscription = getStreamCoordinatorClient().addListener(streamName, new StreamPropertyListener() {
        @Override
        public void generationChanged(String streamName, int generation) {
          streamInitSize.set(0);
        }
      });
    }

    @Override
    public void cancel() {
      truncationSubscription.cancel();
    }

    /**
     * Check that the aggregated size of the heartbeats received by all Stream writers is higher than some threshold.
     * If it is, we publish a notification.
     */
    public void checkAggregatedSize() {
      long sum = streamInitSize.get() + streamWriterSizeCollector.getTotalCollected(streamName);
      if (isInit || sum - streamBaseCount.get() > Constants.Notification.Stream.DEFAULT_DATA_THRESHOLD) {
        try {
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
