/*
* Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.notifications.service.kafka; // tms

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.TransactionSystemClientService;
import co.cask.cdap.messaging.MessageFetcher;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.messaging.data.RawMessage;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.NotificationFeedNotFoundException;
import co.cask.cdap.notifications.service.AbstractNotificationService;
import co.cask.cdap.notifications.service.NotificationException;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NotificationFeedId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of the {@link NotificationService} that uses TMS as the transport.
 */
public class MessagingNotificationService extends AbstractNotificationService {
  private static final Logger LOG = LoggerFactory.getLogger(MessagingNotificationService.class);

  private final MessagingService messagingService;
  private final TopicId notificationTopic;
  private final AtomicBoolean needFetch;

  // Executor to publish notifications to TMS
  private ListeningExecutorService publishingExecutor;
  private ScheduledExecutorService subscribeExecutor;

  @Inject
  MessagingNotificationService(CConfiguration cConf, DatasetFramework dsFramework,
                               TransactionSystemClientService transactionSystemClient,
                               NotificationFeedManager feedManager, MessagingService messagingService) {
    super(dsFramework, transactionSystemClient, feedManager);
    this.messagingService = messagingService;
    this.needFetch = new AtomicBoolean(false);
    this.notificationTopic = NamespaceId.SYSTEM.topic(cConf.get(Constants.Notification.TOPIC));
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();
    publishingExecutor = MoreExecutors.listeningDecorator(
      Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("notification-publisher")));
    subscribeExecutor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("notification-subscriber"));
  }

  @Override
  protected void shutDown() throws Exception {
    publishingExecutor.shutdownNow();
    subscribeExecutor.shutdownNow();
    super.shutDown();
  }

  @Override
  public <N> ListenableFuture<N> publish(final NotificationFeedId feed, final N notification,
                                         final Type notificationType) throws NotificationException {
    LOG.trace("Publishing on notification feed [{}]: {}", feed, notification);
    return publishingExecutor.submit(new Callable<N>() {
      @Override
      public N call() throws Exception {
        try {
          NotificationMessage message = new NotificationMessage(feed, GSON.toJsonTree(notification, notificationType));
          messagingService.publish(StoreRequestBuilder.of(notificationTopic).addPayloads(GSON.toJson(message)).build());
          return notification;
        } catch (Exception e) {
          throw new NotificationException(e);
        }
      }
    });
  }

  @Override
  public <N> Cancellable subscribe(NotificationFeedId feed, NotificationHandler<N> handler,
                                   Executor executor)
    throws NotificationFeedNotFoundException, NotificationFeedException {

    Cancellable subscribeCancellable = super.subscribe(feed, handler, executor);

    // If already has a thread fetching, just return the cancellable.
    if (!needFetch.compareAndSet(false, true)) {
      return subscribeCancellable;
    }

    // Start fetching
    subscribeExecutor.execute(new Runnable() {

      private final long startTime = System.currentTimeMillis();
      private final RetryStrategy scheduleStrategy = RetryStrategies.exponentialDelay(100, 3000, TimeUnit.MILLISECONDS);
      private byte[] messageId;
      private int emptyFetchCount;

      @Override
      public void run() {
        try {
          MessageFetcher fetcher = messagingService.prepareFetch(notificationTopic);
          if (messageId == null) {
            fetcher.setStartTime(startTime);
          } else {
            fetcher.setStartMessage(messageId, false);
          }

          emptyFetchCount++;
          try (CloseableIterator<RawMessage> iterator = fetcher.fetch()) {
            while (iterator.hasNext()) {
              emptyFetchCount = 0;
              RawMessage rawMessage = iterator.next();
              NotificationMessage message = GSON.fromJson(new String(rawMessage.getPayload(), StandardCharsets.UTF_8),
                                                          NotificationMessage.class);
              try {
                LOG.trace("Decoded notification: {}", message);
                notificationReceived(message.getFeedId(), message.getNotificationJson());
              } catch (Throwable t) {
                LOG.warn("Error while processing notification {} with handler {}", message, t);
              }
              messageId = rawMessage.getId();
            }
          }
        } catch (Exception e) {
          LOG.error("Failed to get notification", e);
        }

        // Back-off if it was empty fetch.
        if (emptyFetchCount > 0) {
          // Schedule the next fetch. Exponential strategy doesn't use the time component,
          // so doesn't matter what we passed in
          subscribeExecutor.schedule(this,
                                     scheduleStrategy.nextRetry(emptyFetchCount, startTime), TimeUnit.MILLISECONDS);
        } else {
          subscribeExecutor.execute(this);
        }
      }
    });

    return subscribeCancellable;
  }
}
