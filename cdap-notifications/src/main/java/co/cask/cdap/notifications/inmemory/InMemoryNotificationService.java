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

import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.notifications.BasicNotificationContext;
import co.cask.cdap.notifications.NotificationFeed;
import co.cask.cdap.notifications.NotificationHandler;
import co.cask.cdap.notifications.client.AbstractNotificationSubscriber;
import co.cask.cdap.notifications.kafka.KafkaNotificationSubscriber;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In memory Notification service that pushes notifications to subscribers.
 */
public class InMemoryNotificationService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaNotificationSubscriber.class);
  private static final int EXECUTOR_POOL_SIZE = 10;

  private final SetMultimap<NotificationFeed, AbstractNotificationSubscriber> feedsToSubscribers;
  private final ReadWriteLock lock;

  private final DatasetFramework dsFramework;
  private final TransactionSystemClient transactionSystemClient;

  private ListeningExecutorService executor;

  @Inject
  public InMemoryNotificationService(DatasetFramework dsFramework, TransactionSystemClient transactionSystemClient) {
    this.dsFramework = dsFramework;
    this.transactionSystemClient = transactionSystemClient;
    this.feedsToSubscribers = HashMultimap.create();
    this.lock = new ReentrantReadWriteLock();
  }

  @Override
  protected void startUp() throws Exception {
    executor = MoreExecutors.listeningDecorator(
      Executors.newFixedThreadPool(EXECUTOR_POOL_SIZE, Threads.createDaemonThreadFactory("notification-service-%d")));
  }

  @Override
  protected void shutDown() throws Exception {
    executor.shutdownNow();
  }

  /**
   * Publish a {@code notification} on a {@code feed}. Internally pushes the notification to all subscribers of the
   * feed.
   *
   * @param feed {@link co.cask.cdap.notifications.NotificationFeed} on which to publish the {@code notification}.
   * @param notification Notification to publish.
   * @param <N> Type of the notification to publish.
   * @return A {@link ListenableFuture} describing the pushing of the notification.
   */
  public <N> ListenableFuture<Void> publish(final NotificationFeed feed, final N notification) {
    lock.readLock().lock();
    try {
      Set<AbstractNotificationSubscriber> subscribers = feedsToSubscribers.get(feed);
      if (subscribers == null) {
        return Futures.immediateFuture(null);
      }

      for (final AbstractNotificationSubscriber subscriber : subscribers) {
        if (!subscriber.isConsuming()) {
          continue;
        }

        final NotificationHandler handler = subscriber.getFeedMap().get(feed);
        executor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              handler.processNotification(notification,
                                          new BasicNotificationContext(dsFramework, transactionSystemClient));
            } catch (Throwable t) {
              LOG.warn("Notification {} on feed {} could not be processed successfully by subscriber {}",
                       notification, feed, subscriber, t);
            }
          }
        });
      }
    } finally {
      lock.readLock().unlock();
    }
    return Futures.immediateFuture(null);
  }

  /**
   * Make an {@link AbstractNotificationSubscriber} subscribe to the Notifications sent to a {@link NotificationFeed}.
   *
   * @param subscriber Notification subscriber
   * @param feed {@link NotificationFeed} to subscribe to.
   */
  public void subscribe(AbstractNotificationSubscriber subscriber, NotificationFeed feed) {
    lock.writeLock().lock();
    try {
      feedsToSubscribers.put(feed, subscriber);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Cancel all subscriptions of one subscriber.
   *
   * @param subscriber subscriber that wants to cancel its subscriptions.
   */
  public void cancel(AbstractNotificationSubscriber subscriber) {
    lock.writeLock().lock();
    try {
      for (NotificationFeed feed : feedsToSubscribers.keySet()) {
        // Here the subscriber will be identified by its reference, since equals() is not overridden.
        // Which is good, this is the behavior we expect.
        feedsToSubscribers.remove(feed, subscriber);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }
}
