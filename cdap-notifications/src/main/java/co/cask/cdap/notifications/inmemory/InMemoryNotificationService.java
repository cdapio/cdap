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
import co.cask.cdap.notifications.client.NotificationFeedClient;
import co.cask.cdap.notifications.client.NotificationService;
import co.cask.cdap.notifications.service.NotificationException;
import co.cask.cdap.notifications.service.NotificationFeedException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In memory Notification service that pushes notifications to subscribers.
 */
public class InMemoryNotificationService extends AbstractIdleService implements NotificationService {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryNotificationService.class);
  private static final int EXECUTOR_POOL_SIZE = 10;

  private final SetMultimap<NotificationFeed, NotificationHandler> feedsToHandlers;
  private final Map<NotificationHandler, Executor> handlersToExecutors;
  private final ReadWriteLock lock;

  private final DatasetFramework dsFramework;
  private final TransactionSystemClient transactionSystemClient;
  private final NotificationFeedClient feedClient;

  private ListeningExecutorService defaultExecutor;

  @Inject
  public InMemoryNotificationService(DatasetFramework dsFramework, TransactionSystemClient transactionSystemClient,
                                     NotificationFeedClient feedClient) {
    this.dsFramework = dsFramework;
    this.transactionSystemClient = transactionSystemClient;
    this.feedClient = feedClient;
    this.feedsToHandlers = HashMultimap.create();
    this.handlersToExecutors = Maps.newHashMap();
    this.lock = new ReentrantReadWriteLock();
  }

  @Override
  protected void startUp() throws Exception {
    defaultExecutor = MoreExecutors.listeningDecorator(
      Executors.newFixedThreadPool(EXECUTOR_POOL_SIZE, Threads.createDaemonThreadFactory("notification-service-%d")));
  }

  @Override
  protected void shutDown() throws Exception {
    defaultExecutor.shutdownNow();
  }

  @Override
  public <N> ListenableFuture<N> publish(NotificationFeed feed, N notification)
    throws NotificationException, NotificationFeedException {
    return publish(feed, notification, notification.getClass());
  }

  @Override
  public <N> ListenableFuture<N> publish(final NotificationFeed feed, final N notification, Type notificationType)
    throws NotificationException, NotificationFeedException {
    lock.readLock().lock();
    try {
      LOG.debug("Publishing on notification feed [{}]: {}", feed, notification);
      Set<NotificationHandler> handlers = feedsToHandlers.get(feed);
      if (handlers.isEmpty()) {
        return Futures.immediateFuture(null);
      }

      for (final NotificationHandler handler : handlers) {
        handlersToExecutors.get(handler).execute(new Runnable() {
          @Override
          public void run() {
            try {
              handler.processNotification(notification,
                                          new BasicNotificationContext(dsFramework, transactionSystemClient));
            } catch (Throwable t) {
              LOG.warn("Notification {} on feed {} could not be processed successfully by handler {}",
                       notification, feed, handler, t);
            }
          }
        });
      }
    } finally {
      lock.readLock().unlock();
    }
    return Futures.immediateFuture(null);
  }

  @Override
  public <N> Cancellable subscribe(NotificationFeed feed, NotificationHandler<N> handler)
    throws NotificationFeedException {
    return subscribe(feed, handler, defaultExecutor);
  }

  /**
   *
   * @param feed
   * @param handler
   * @param executor The executor passed as parameter will be used to push the notifications published via the
   *                 {@link #publish} methods to the {@code handler}.
   * @param <N>
   * @return
   * @throws NotificationFeedException
   */
  @Override
  public <N> Cancellable subscribe(final NotificationFeed feed, final NotificationHandler<N> handler, Executor executor)
    throws NotificationFeedException {
    // This call will make sure that the feed exists
    feedClient.getFeed(feed);

    lock.writeLock().lock();
    try {
      feedsToHandlers.put(feed, handler);
      handlersToExecutors.put(handler, executor);
    } finally {
      lock.writeLock().unlock();
    }

    return new Cancellable() {
      @Override
      public void cancel() {
        lock.writeLock().lock();
        try {
          feedsToHandlers.remove(feed, handler);
          handlersToExecutors.remove(handler);
        } finally {
          lock.writeLock().unlock();
        }
      }
    };
  }
}
