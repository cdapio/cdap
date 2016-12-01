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
import co.cask.cdap.notifications.service.NotificationException;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NotificationFeedId;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

/**
 * Implementation of {@link HeartbeatPublisher} that publishes {@link StreamWriterHeartbeat} as notification using
 * the {@link NotificationService}.
 */
public class NotificationHeartbeatPublisher extends AbstractIdleService implements HeartbeatPublisher {

  private final NotificationService notificationService;
  private final NotificationFeedId heartbeatFeed;

  @Inject
  public NotificationHeartbeatPublisher(NotificationService notificationService) {
    this.notificationService = notificationService;
    this.heartbeatFeed = new NotificationFeedId(
      NamespaceId.SYSTEM.getNamespace(),
      Constants.Notification.Stream.STREAM_INTERNAL_FEED_CATEGORY,
      Constants.Notification.Stream.STREAM_HEARTBEAT_FEED_NAME);
  }

  @Override
  protected void startUp() throws Exception {
    notificationService.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    notificationService.stopAndWait();
  }

  @Override
  public ListenableFuture<StreamWriterHeartbeat> sendHeartbeat(StreamWriterHeartbeat heartbeat) {
    try {
      return notificationService.publish(heartbeatFeed, heartbeat);
    } catch (NotificationException e) {
      throw new IllegalArgumentException("Streams' heartbeat notification feed has not been created", e);
    }
  }
}
