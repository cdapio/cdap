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

package co.cask.cdap.notifications.service;

import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.NotificationFeedNotFoundException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.twill.common.Cancellable;

import java.lang.reflect.Type;
import java.util.concurrent.Executor;

/**
 * A Notification service for publishing and subscribing to notifications.
 */
public interface NotificationService extends Service {

  /**
   * Send one Notification asynchronously. The class of the {@code notification} is used to serialize the message
   * passed to the Notification system.
   *
   * @param feed {@link NotificationFeed} where to publish the notification.
   * @param notification notification object to send.
   * @param <N> Type of the notification to send.
   * @return a {@link ListenableFuture} describing the state of the async send operation.
   * @throws NotificationException in case of any error when publishing the notification.
   * @throws NotificationFeedException in case of any error regarding the {@code feed}.
   */
  <N> ListenableFuture<N> publish(NotificationFeed feed, N notification)
    throws NotificationException, NotificationFeedException;

  /**
   * Send one Notification asynchronously. The {@code notificationType} is used to serialize the notification
   * passed to the Notification system.
   *
   * @param feed {@link NotificationFeed} where to publish the notification.
   * @param notification notification object to send.
   * @param notificationType type to use to serialize the notification in the Notification system.
   * @param <N> Type of the notification to send.
   * @return a {@link ListenableFuture} describing the state of the async send operation.
   * @throws NotificationException in case of any error when publishing the notification.
   * @throws NotificationFeedException in case of any error regarding the {@code feed}.
   */
  <N> ListenableFuture<N> publish(NotificationFeed feed, N notification, Type notificationType)
    throws NotificationException, NotificationFeedException;

  /**
   * Subscribe to the notification received on the {@code feed}, and handle the notifications with the {@code handler}.
   * Before this call is made, the {@code feed} has to be created using the  {@link NotificationFeedManager}.
   * Multiple subscriptions to a same feed with different handlers are possible.
   *
   * @param feed {@link NotificationFeed} to subscribe to.
   * @param handler {@link NotificationHandler} that will handle the notifications coming from the feed.
   * @param <N> Type of the notifications.
   * @return A {@link Cancellable} for cancelling Notification consumption.
   * @throws NotificationFeedNotFoundException if the feed does not exist, according to the
   * {@link NotificationFeedManager}.
   * @throws NotificationFeedException in case of any other error concerning the feed.
   */
  <N> Cancellable subscribe(NotificationFeed feed, NotificationHandler<N> handler)
    throws NotificationFeedException;

  /**
   * Subscribe to the notification received on the {@code feed}, and handle the notifications with the {@code handler}.
   * Before this call is made, the {@code feed} has to be created using the {@link NotificationFeedManager}.
   * Multiple subscriptions to a same feed with different handlers are possible.
   *
   * @param feed {@link NotificationFeed} to subscribe to.
   * @param handler {@link NotificationHandler} that will handle the notifications coming from the feed.
   * @param executor {@link Executor} to use to perform the polling/pushing of notifications from the Notification
   *                 system.
   * @param <N> Type of the notifications.
   * @return A {@link Cancellable} for cancelling Notification consumption.
   * @throws NotificationFeedNotFoundException if the feed does not exist, according to the
   * {@link NotificationFeedManager}.
   * @throws NotificationFeedException in case of any other error concerning the feed.
   */
  <N> Cancellable subscribe(NotificationFeed feed, NotificationHandler<N> handler, Executor executor)
    throws NotificationFeedException;
}
