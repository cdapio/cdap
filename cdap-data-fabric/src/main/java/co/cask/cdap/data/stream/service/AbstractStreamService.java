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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;

/**
 * Stream service meant to run in an HTTP service.
 */
public abstract class AbstractStreamService extends AbstractIdleService implements StreamService {

  private final StreamCoordinatorClient streamCoordinatorClient;
  private final StreamFileJanitorService janitorService;
  private final StreamWriterSizeManager sizeManager;
  private final NotificationFeedManager feedManager;

  /**
   * Children classes should implement this method to add logic to the start of this {@link Service}.
   *
   * @throws Exception in case of any error while initializing
   */
  protected abstract void initialize() throws Exception;

  /**
   * Children classes should implement this method to add logic to the shutdown of this {@link Service}.
   *
   * @throws Exception in case of any error while shutting down
   */
  protected abstract void doShutdown() throws Exception;

  protected AbstractStreamService(StreamCoordinatorClient streamCoordinatorClient,
                                  StreamFileJanitorService janitorService,
                                  StreamWriterSizeManager sizeManager,
                                  NotificationFeedManager feedManager) {
    this.streamCoordinatorClient = streamCoordinatorClient;
    this.janitorService = janitorService;
    this.sizeManager = sizeManager;
    this.feedManager = feedManager;
  }

  @Override
  protected final void startUp() throws Exception {
    createHeartbeatsFeed();

    streamCoordinatorClient.startAndWait();
    janitorService.startAndWait();
    sizeManager.startAndWait();
    sizeManager.initialize();

    initialize();
  }

  @Override
  protected final void shutDown() throws Exception {
    doShutdown();
    sizeManager.stopAndWait();
    janitorService.stopAndWait();
    streamCoordinatorClient.stopAndWait();
  }

  /**
   * Create Notification feed for stream's heartbeats, if it does not already exist.
   */
  private void createHeartbeatsFeed() throws NotificationFeedException {
    // TODO worry about namespaces here. Should we create one heartbeat feed per namespace?
    NotificationFeed streamHeartbeatsFeed = new NotificationFeed.Builder()
      .setNamespace("default")
      .setCategory(Constants.Notification.Stream.STREAM_HEARTBEAT_FEED_CATEGORY)
      .setName(Constants.Notification.Stream.STREAM_HEARTBEAT_FEED_NAME)
      .setDescription("Streams heartbeats feed.")
      .build();

    try {
      feedManager.getFeed(streamHeartbeatsFeed);
    } catch (NotificationFeedException e) {
      feedManager.createFeed(streamHeartbeatsFeed);
    }
  }
}
