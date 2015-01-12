/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.notifications.service.inmemory;

import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.service.BasicNotificationContext;
import co.cask.cdap.notifications.service.NotificationContext;
import co.cask.cdap.notifications.service.NotificationException;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.concurrent.Executor;

/**
 * In-memory Notification service that pushes notifications to subscribers.
 */
public class InMemoryNotificationService extends AbstractIdleService implements NotificationService {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryNotificationService.class);
  private static final Gson GSON = new Gson();

  private final Multimap<NotificationFeed, NotificationCaller<?>> subscribers;

  private final DatasetFramework dsFramework;
  private final TransactionSystemClient transactionSystemClient;
  private final NotificationFeedManager feedManager;

  @Inject
  public InMemoryNotificationService(DatasetFramework dsFramework, TransactionSystemClient transactionSystemClient,
                                     NotificationFeedManager feedManager) {
    this.dsFramework = dsFramework;
    this.transactionSystemClient = transactionSystemClient;
    this.feedManager = feedManager;
    this.subscribers = Multimaps.synchronizedMultimap(HashMultimap.<NotificationFeed, NotificationCaller<?>>create());
  }

  @Override
  protected void startUp() throws Exception {
    // No-op
  }

  @Override
  protected void shutDown() throws Exception {
    // No-op
  }

  @Override
  public <N> ListenableFuture<N> publish(NotificationFeed feed, N notification)
    throws NotificationException {
    return publish(feed, notification, notification.getClass());
  }

  @Override
  public <N> ListenableFuture<N> publish(NotificationFeed feed, N notification, Type notificationType)
    throws NotificationException {
    Collection<NotificationCaller<?>> callers = subscribers.get(feed);
    synchronized(subscribers) {
      callers = ImmutableList.copyOf(callers);
    }
    for (NotificationCaller caller : callers) {
      caller.received(notification, new BasicNotificationContext(dsFramework, transactionSystemClient));
    }
    return Futures.immediateFuture(notification);
  }

  /**
   * Publishes a notification as a {@link JsonElement} to a {@code feed}.
   *
   * @param feed feed to publish notification to
   * @param notificationJson json element representing the notification to publish
   * @return a future describing the state of publishing this notification to all subscribers.
   * @see #publish(NotificationFeed, Object, Type)
   */
  public ListenableFuture<JsonElement> publish(NotificationFeed feed, JsonElement notificationJson) {
    Collection<NotificationCaller<?>> callers = subscribers.get(feed);
    synchronized(subscribers) {
      callers = ImmutableList.copyOf(callers);
    }
    for (NotificationCaller caller : callers) {
      Object notification = GSON.fromJson(notificationJson, caller.getNotificationFeedType());
      caller.received(notification, new BasicNotificationContext(dsFramework, transactionSystemClient));
    }
    return Futures.immediateFuture(notificationJson);
  }

  @Override
  public <N> Cancellable subscribe(NotificationFeed feed, NotificationHandler<N> handler)
    throws NotificationFeedException {
    return subscribe(feed, handler, Threads.SAME_THREAD_EXECUTOR);
  }

  // The executor passed as parameter will be used to push the notifications published via the
  // publish methods to the handler.
  @Override
  public <N> Cancellable subscribe(NotificationFeed feed, NotificationHandler<N> handler, Executor executor)
    throws NotificationFeedException {
    // This call will make sure that the feed exists
    feedManager.getFeed(feed);

    final NotificationCaller<N> caller = new NotificationCaller<N>(feed, handler, executor);
    subscribers.put(feed, caller);
    return caller;
  }

  /**
   * Wrapper around a {@link NotificationHandler}, containing a reference to a {@link NotificationHandler}
   * and a {@link NotificationFeed}.
   *
   * @param <N> Type of the Notification to handle
   */
  private class NotificationCaller<N> implements NotificationHandler<N>, Cancellable {
    private final NotificationFeed feed;
    private final NotificationHandler<N> handler;
    private final Executor executor;
    private volatile boolean completed;

    NotificationCaller(NotificationFeed feed, NotificationHandler<N> handler, Executor executor) {
      this.feed = feed;
      this.handler = handler;
      this.executor = executor;
    }

    @Override
    public Type getNotificationFeedType() {
      return handler.getNotificationFeedType();
    }

    @Override
    public void received(final N notification, final NotificationContext notificationContext) {
      if (completed) {
        return;
      }
      executor.execute(new Runnable() {
        @Override
        public void run() {
          if (completed) {
            return;
          }
          try {
            handler.received(notification, notificationContext);
          } catch (Throwable t) {
            LOG.warn("Notification {} on feed {} could not be processed successfully by handler {}",
                     notification, feed, handler, t);
          }
        }
      });
    }

    @Override
    public void cancel() {
      completed = true;
      subscribers.remove(feed, this);
    }
  }
}
