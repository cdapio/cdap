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

package co.cask.cdap.notifications.client;

import co.cask.cdap.notifications.NotificationFeed;
import co.cask.cdap.notifications.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationFeedException;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.common.Cancellable;

import java.io.IOException;
import java.util.List;

/**
 * A client that can publish and subscribe to notifications.
 * <p/>
 * Typical usage of publishing example:
 * <tt>
 *   NotificationClient client = ...;
 *   NotificationClient.Publisher<String> publisher = client.createPublisher(feed, String.class);
 *   publisher.publish("foobar");
 *   publisher.shutdownNow()
 * </tt>
 * The same instance of this class can be used to create multiple {@link Publisher}s using the {@link #createPublisher}
 * method.
 * <p/>
 * Typical usage of subscribing example:
 * <tt>
 *   NotificationClient client = ...;
 *   NotificationClient.Subscriber subscriber = client.createSubscriber();
 *   subscriber.add(feed, new NotificationHandler<String>() {
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
 *   Cancellable cancellable = subscriber.consume();
 *   ...
 *   cancellable.cancel();
 * </tt>
 */
public interface NotificationClient {
  /**
   * Creates a {@link Publisher} object. Before this call is made, the {@code feed} has to be created using the
   * {@link co.cask.cdap.notifications.NotificationFeedManager}.
   *
   * @param <N> Type of the Notification published.
   * @param feed Identity of the feed.
   * @return a {@link Publisher} object.
   * @throws NotificationFeedException if anything wrong happens when trying to build a {@link Publisher} object.
   */
  <N> Publisher<N> createPublisher(NotificationFeed feed) throws NotificationFeedException;

  /**
   * Prepares for Notification consumption.
   *
   * @return A {@link Subscriber} to setup details about message consumption.
   */
  Subscriber createSubscriber();

  /**
   * Object responsible for sending Notifications to one {@link NotificationFeed}.
   *
   * @param <N> Type of the Notification published.
   */
  interface Publisher<N> {

    /**
     * Send one Notification asynchronously.
     *
     * @param notification notification object to send, which type is set by the generic parameter of {@link Publisher}.
     * @return a {@link ListenableFuture} describing the state of the async send operation.
     * @throws java.io.IOException if the Notification could not be serialized to the Notification System successfully.
     * @throws PublisherShutdownException if this {@link Publisher} is already shut down.
     */
    ListenableFuture<N> publish(N notification) throws IOException, PublisherShutdownException;

    /**
     * Initiates an orderly shutdown in which previously submitted notification are sent,
     * but no new notifications will be accepted. Invocation has no additional effect if already shut down.
     *
     * @see java.util.concurrent.ExecutorService#shutdown
     */
    void shutdown();

    /**
     * Attempts to stop all actively sending notifications, halts the processing of waiting notifications,
     * and returns a list of the notifications that were awaiting execution.
     *
     * @return list of notifications that never commenced sending.
     *
     * @see java.util.concurrent.ExecutorService#shutdownNow
     */
    List<N> shutdownNow();

    /**
     * Returns true if this executor has been shut down.
     *
     * @return true if this executor has been shut down.
     */
    boolean isShutdown();
  }

  /**
   * A builder for preparing Notifications consumption.
   */
  interface Subscriber {

    /**
     * Add a subscription to a {@link NotificationFeed}. Notifications coming from that {@code feed} will be
     * handled by the {@code handler}.
     * Before this call is made, the {@code feed} has to be created using the
     * {@link co.cask.cdap.notifications.NotificationFeedManager}.
     * The subscriptions are not active until the {@link #consume} method is called.
     * Adding a subscription to a {@link NotificationFeed} that already exists will overwrite the previous
     * {@code handler} that was attached to the feed.
     *
     * @param feed {@link NotificationFeed} to subscribe to.
     * @param handler {@link NotificationHandler} that will handle the notifications coming from the feed.
     * @param <N> Type of the Notifications.
     * @return This {@link Subscriber} instance.
     * @throws co.cask.cdap.notifications.service.NotificationFeedNotFoundException if the feed does not exist,
     * according to the {@link co.cask.cdap.notifications.NotificationFeedManager}.
     * @throws NotificationFeedException in case the {@link #consume} method has already been called,
     * or in case of any other error
     */
    <N> Subscriber add(NotificationFeed feed, NotificationHandler<N> handler) throws NotificationFeedException;

    // TODO add other add method with executor, not until
    // KafkaPreparer.Preparer.consume accepts an Executor itself

    /**
     * Starts the consumption of Notification as being configured by this {@link Subscriber}.
     * The consumption of all {@link NotificationFeed}s is handled by one single threaded executor.
     *
     * @return A {@link Cancellable} for cancelling Notification consumption.
     */
    Cancellable consume();
  }

  /**
   * Exception thrown when an operation on a {@link Publisher} is forbidden because it has been shut down.
   */
  public static class PublisherShutdownException extends Exception {
    public PublisherShutdownException(String s) {
      super(s);
    }
  }
}
