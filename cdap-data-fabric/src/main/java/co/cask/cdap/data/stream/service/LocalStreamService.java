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
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.service.NotificationService;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Stream service running in local mode.
 */
public class LocalStreamService extends AbstractStreamService {
  private static final Logger LOG = LoggerFactory.getLogger(LocalStreamService.class);

  private final NotificationService notificationService;
  private final StreamAdmin streamAdmin;
  private final StreamWriterSizeCollector streamWriterSizeCollector;
  private final StreamMetaStore streamMetaStore;
  private final Map<String, Long> streamsInitCounts;
  private final Map<String, Long> streamsBaseCounts;
  private boolean isInit;

  @Inject
  public LocalStreamService(StreamCoordinatorClient streamCoordinatorClient,
                            StreamFileJanitorService janitorService,
                            StreamMetaStore streamMetaStore,
                            StreamAdmin streamAdmin,
                            StreamWriterSizeCollector streamWriterSizeCollector,
                            NotificationFeedManager notificationFeedManager,
                            NotificationService notificationService) {
    super(streamCoordinatorClient, janitorService, notificationFeedManager);
    this.streamAdmin = streamAdmin;
    this.streamMetaStore = streamMetaStore;
    this.streamWriterSizeCollector = streamWriterSizeCollector;
    this.notificationService = notificationService;
    this.streamsBaseCounts = Maps.newHashMap();
    this.streamsInitCounts = Maps.newHashMap();
    isInit = true;
  }

  @Override
  protected void initialize() throws Exception {
    for (StreamSpecification streamSpec : streamMetaStore.listStreams()) {
      long filesSize = 0;
      try {
        StreamConfig config = streamAdmin.getConfig(streamSpec.getName());
        filesSize = StreamUtils.fetchStreamFilesSize(config);
      } catch (IOException e) {
        LOG.error("Could not compute sizes of files for stream {}", streamSpec.getName());
      }
      streamsInitCounts.put(streamSpec.getName(), filesSize);
      streamsBaseCounts.put(streamSpec.getName(), filesSize);
    }
  }

  @Override
  protected void doShutdown() throws Exception {
    // No-op
  }

  @Override
  protected void runOneIteration() throws Exception {
    // Get stream size - which will be the entire size - and send a notification if the size is big enough
    for (StreamSpecification streamSpec : streamMetaStore.listStreams()) {
      Long initSize = streamsInitCounts.get(streamSpec.getName());
      if (initSize == null) {
        // First time that we see this Stream here
        initSize = (long) 0;
        streamsInitCounts.put(streamSpec.getName(), initSize);
        streamsBaseCounts.put(streamSpec.getName(), initSize);
      }
      long absoluteSize = initSize + streamWriterSizeCollector.getTotalCollected(streamSpec.getName());

      if (isInit || absoluteSize - streamsBaseCounts.get(streamSpec.getName()) >
        Constants.Notification.Stream.DEFAULT_DATA_THRESHOLD) {
        try {
          publishNotification(streamSpec.getName(), absoluteSize);
        } finally {
          streamsBaseCounts.put(streamSpec.getName(), absoluteSize);
        }
      }
    }
    isInit = false;
  }

  private void publishNotification(String streamName, long absoluteSize) {
    NotificationFeed streamFeed = new NotificationFeed.Builder()
      .setNamespace("default")
      .setCategory(Constants.Notification.Stream.STREAM_FEED_CATEGORY)
      .setName(streamName)
      .build();
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
