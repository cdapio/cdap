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
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.service.AbstractNotificationService;
import co.cask.cdap.notifications.service.NotificationException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.common.Threads;

import java.lang.reflect.Type;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

/**
 * In-memory Notification service that pushes notifications to subscribers.
 */
public class InMemoryNotificationService extends AbstractNotificationService {
  private static final Gson GSON = new Gson();
  private ListeningExecutorService executorService;

  @Inject
  public InMemoryNotificationService(DatasetFramework dsFramework, TransactionSystemClient transactionSystemClient,
                                     NotificationFeedManager feedManager) {
    super(dsFramework, transactionSystemClient, feedManager);
  }

  @Override
  protected void startUp() throws Exception {
    executorService = MoreExecutors.listeningDecorator(
      Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("notification-publisher-executor")));
  }

  @Override
  protected void shutDown() throws Exception {
    executorService.shutdownNow();
  }

  @Override
  public <N> ListenableFuture<N> publish(final NotificationFeed feed, final N notification, final Type notificationType)
    throws NotificationException {
    return executorService.submit(new Callable<N>() {
      @Override
      public N call() throws Exception {
        notificationReceived(feed, GSON.toJsonTree(notification, notificationType));
        return notification;
      }
    });
  }
}
