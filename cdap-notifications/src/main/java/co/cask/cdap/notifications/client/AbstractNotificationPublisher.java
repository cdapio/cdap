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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Common implementation of a {@link co.cask.cdap.notifications.client.NotificationClient.Publisher}.
 */
public abstract class AbstractNotificationPublisher<N> implements NotificationClient.Publisher<N> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractNotificationPublisher.class);

  private final ExecutorService executor;
  private final NotificationFeed feed;

  protected AbstractNotificationPublisher(NotificationFeed feed) {
    this.executor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("notification-sender-%d"));
    this.feed = feed;
  }

  protected abstract ListenableFuture<Void> doSend(N notification) throws IOException;

  @Override
  public synchronized void shutdown() {
    executor.shutdown();
  }

  @Override
  public synchronized boolean isShutdown() {
    return executor.isShutdown();
  }

  @Override
  public synchronized List<N> shutdownNow() {
    List<Runnable> runnables = executor.shutdownNow();
    ImmutableList.Builder<N> builder = ImmutableList.builder();
    for (Runnable r : runnables) {
      if (r instanceof GetNotificationRunnable) {
        builder.add(((GetNotificationRunnable<N>) r).getNotification());
      }
    }
    return builder.build();
  }

  @Override
  public synchronized ListenableFuture<Void> publish(N notification)
    throws IOException, NotificationClient.PublisherShutdownException {
    if (isShutdown()) {
      LOG.warn("Cannot publish notification, sender is shut down.");
      throw new NotificationClient.PublisherShutdownException(
        "Tried to publish notification " + notification + " after shut down.");
    }
    LOG.debug("Publishing on notification feed [{}]: {}", feed, notification);
    ListenableFuture<Void> future = doSend(notification);
    executor.submit(new GetNotificationRunnable<N>(future, notification));
    return future;
  }

  /**
   * Runnable which only job is to wait for the get method of a future representing the pushing of Notification
   * to return.
   *
   * @param <N> Type of the Notification being pushed.
   */
  private static final class GetNotificationRunnable<N> implements Runnable {

    private final ListenableFuture<?> future;
    private final N notification;

    public GetNotificationRunnable(ListenableFuture<?> future, N notification) {
      this.future = future;
      this.notification = notification;
    }

    public N getNotification() {
      return notification;
    }

    @Override
    public void run() {
      try {
        future.get();
      } catch (Exception e) {
        LOG.error("Could not get publish notification result", e);
        Throwables.propagate(e);
      }
    }
  }
}
