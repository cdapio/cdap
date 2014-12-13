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

package co.cask.cdap.notifications;

import co.cask.cdap.notifications.service.NotificationFeedException;
import org.apache.twill.common.Cancellable;

/**
 * Notification subscriber. Typical usage example:
 * <tt>
 *   NotificationSubscriber subscriber = ...;
 *   NotificationSubscriber.Preparer preparer = subscriber.prepare();
 *   preparer.add(feed, new NotificationHandler<String>() {
 *     @Override
 *     public Type getNotificationFeedType() {
 *       return String.class;
 *     }
 *
 *     @Override
 *     public void processNotification(String notification, NotificationContext notificationContext) {
 *       // ...
 *     }
 *   });
 *   Cancellable cancellable = preparer.consume();
 *   ...
 *   cancellable.cancel();
 * </tt>
 */
public interface NotificationSubscriber {

  /**
   * A builder for preparing Notifications consumption.
   */
  interface Preparer {

    /**
     * Add a subscription to a {@link NotificationFeed}. Notifications coming from that {@code feed} will be
     * handled by the {@code handler}.
     * Before this call is made, the {@code feed} has to be created using the {@link NotificationFeedManager}.
     * The subscriptions are not active until the {@link #consume} method is called.
     * Adding a subscription to a {@link NotificationFeed} that already exists will overwrite the previous
     * {@code handler} that was attached to the feed.
     *
     * @param feed {@link NotificationFeed} to subscribe to.
     * @param handler {@link NotificationHandler} that will handle the notifications coming from the feed.
     * @param <N> Type of the Notifications.
     * @return This {@link Preparer} instance.
     * @throws co.cask.cdap.notifications.service.NotificationFeedNotFoundException if the feed does not exist,
     * according to the {@link NotificationFeedManager}.
     * @throws NotificationFeedException in case the {@link #consume} method has already been called,
     * or in case of any other error
     */
    <N> Preparer add(NotificationFeed feed, NotificationHandler<N> handler) throws NotificationFeedException;

    // TODO add other add method with executor, not until
    // KafkaPreparer.Preparer.consume accepts an Executor itself

    /**
     * Starts the consumption of Notification as being configured by this {@link Preparer}.
     *
     * @return A {@link Cancellable} for cancelling Notification consumption.
     */
    Cancellable consume();
  }

  /**
   * Prepares for Notification consumption.
   *
   * @return A {@link Preparer} to setup details about message consumption.
   */
  Preparer prepare();
}
