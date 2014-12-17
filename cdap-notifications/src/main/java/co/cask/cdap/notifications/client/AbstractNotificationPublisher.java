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
import co.cask.cdap.notifications.service.NotificationException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Common implementation of a {@link co.cask.cdap.notifications.client.NotificationClient.Publisher}.
 *
 * @param <N> Type of the Notifications to publish.
 */
public abstract class AbstractNotificationPublisher<N> implements NotificationClient.Publisher<N> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractNotificationPublisher.class);

  private final ListeningExecutorService executor;
  private final NotificationFeed feed;

  /**
   * Publish the {@code notification} to the Notification service synchronously,
   * which handles the storing of notifications.
   *
   * @param notification notification to publish.
   * @return A {@link ListenableFuture} describing the pushing of the notification to the service.
   * @throws NotificationException if any error happens when publishing the Notification.
   */
  protected abstract void doPublish(N notification) throws NotificationException;

  protected AbstractNotificationPublisher(NotificationFeed feed) {
    this.executor = MoreExecutors.listeningDecorator(
      Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("notification-sender-%d")));
    this.feed = feed;
  }

  public NotificationFeed getFeed() {
    return feed;
  }

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
    for (GetNotificationRunnable r : Iterables.filter(runnables, GetNotificationRunnable.class)) {
      builder.add(r.getNotification());
    }
    return builder.build();
  }

  @Override
  public synchronized ListenableFuture<N> publish(final N notification)
    throws IOException, NotificationClient.PublisherShutdownException {
    if (isShutdown()) {
      throw new IllegalStateException("Tried to publish notification " + notification + " after shut down.");
    }
    LOG.debug("Publishing on notification feed [{}]: {}", feed, notification);
    return executor.submit(new GetNotificationRunnable(notification), notification);
  }

  /**
   * Runnable which only job is to wait for the get method of a future representing the pushing of Notification
   * to return.
   */
  private final class GetNotificationRunnable implements Runnable {

    private final N notification;

    public GetNotificationRunnable(N notification) {
      this.notification = notification;
    }

    public N getNotification() {
      return notification;
    }

    @Override
    public void run() {
      try {
        doPublish(notification);
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
  }
}
