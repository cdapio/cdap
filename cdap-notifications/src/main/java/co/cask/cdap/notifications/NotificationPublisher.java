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
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.List;

/**
 * A client that can publish notifications.
 * <p/>
 * Typical usage example:
 * <tt>
 *   NotificationPublisher publisher = ...;
 *   NotificationPublisher.Sender<String> sender = publisher.createSender(feed, String.class);
 *   sender.send("foobar");
 *   sender.shutdownNow()
 * </tt>
 * The same instance of this class can be used to create multiple {@link Sender}s using the {@link #createSender}
 * method.
 */
public interface NotificationPublisher {

  /**
   * Creates a {@link Sender} object.
   * Before this call is made, the {@code feed} has to be created using the {@link NotificationFeedManager}.
   *
   * @param feed Identity of the feed.
   * @param notificationType Type of the Notification published.
   * @param <N> Type of the Notification published.
   * @return a {@link Sender} object.
   * @throws NotificationFeedException if anything wrong happens when trying to build a {@link Sender} object.
   */
  <N> Sender<N> createSender(NotificationFeed feed, Class<N> notificationType) throws NotificationFeedException;

  /**
   * Object responsible for sending Notifications to one {@link NotificationFeed}.
   *
   * @param <N> Type of the Notification published.
   */
  interface Sender<N> {

    /**
     * Send one Notification asynchronously.
     *
     * @param notification notification object to send, which type is set by the generic parameter of {@link Sender}.
     * @return a {@link ListenableFuture} describing the state of the async send operation.
     * @throws IOException if the Notification could not be serialized to the Notification System successfully.
     * @throws SenderShutdownException if this {@link Sender} is already shut down.
     */
    ListenableFuture<Void> send(N notification) throws IOException, SenderShutdownException;

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
   * Exception thrown when an operation on a {@link Sender} is forbidden because it has been shut down.
   */
  public static class SenderShutdownException extends Exception {
    public SenderShutdownException(String s) {
      super(s);
    }
  }
}
