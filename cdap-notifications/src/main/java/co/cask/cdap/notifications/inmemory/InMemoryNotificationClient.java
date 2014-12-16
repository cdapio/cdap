/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.notifications.inmemory;

import co.cask.cdap.notifications.NotificationFeed;
import co.cask.cdap.notifications.client.AbstractNotificationPublisher;
import co.cask.cdap.notifications.client.AbstractNotificationSubscriber;
import co.cask.cdap.notifications.client.NotificationClient;
import co.cask.cdap.notifications.client.NotificationFeedClient;
import co.cask.cdap.notifications.service.NotificationFeedException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;

import java.io.IOException;

/**
 * In memory implementation of the {@link NotificationClient}, which connects to an {@link InMemoryNotificationService}
 * to publish and subscribe.
 */
public class InMemoryNotificationClient implements NotificationClient {

  private final InMemoryNotificationService notificationService;
  private final NotificationFeedClient feedClient;

  @Inject
  public InMemoryNotificationClient(InMemoryNotificationService notificationService,
                                    NotificationFeedClient feedClient) {
    this.notificationService = notificationService;
    this.feedClient = feedClient;
  }

  @Override
  public <N> Publisher<N> createPublisher(NotificationFeed feed) throws NotificationFeedException {
    // This call will make sure that the feed exists - at this point it really should
    // Because only the resource owner can publish changes from the resource
    // And the resource owner should have created the feed first hand.
    feedClient.getFeed(feed);
    return new InMemoryNotificationPublisher<N>(feed);
  }

  @Override
  public Subscriber createSubscriber() {
    return new InMemoryNotificationSubscriber();
  }

  /**
   * In memory implementation of a {@link NotificationClient.Publisher}.
   *
   * @param <N> Type of the notifications published.
   */
  private final class InMemoryNotificationPublisher<N> extends AbstractNotificationPublisher<N> {

    protected InMemoryNotificationPublisher(NotificationFeed feed) {
      super(feed);
    }

    @Override
    protected ListenableFuture<Void> doPublish(N notification) throws IOException {
      return notificationService.publish(getFeed(), notification);
    }
  }

  /**
   * In memory implementation of a {@link NotificationClient.Subscriber}.
   */
  private final class InMemoryNotificationSubscriber extends AbstractNotificationSubscriber {

    protected InMemoryNotificationSubscriber() {
      super(feedClient);
    }

    @Override
    protected void doSubscribe(NotificationFeed feed) {
      notificationService.subscribe(this, feed);
    }

    @Override
    protected Cancellable doConsume() {
      final AbstractNotificationSubscriber thisSubscriber = this;
      return new Cancellable() {
        @Override
        public void cancel() {
          // Remove the subscriber from the notificationService
          notificationService.cancel(thisSubscriber);
        }
      };
    }
  }
}
