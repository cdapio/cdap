/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.TxCallable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.retry.RetryableException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.messaging.MultiThreadMessagingContext;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Abstract class that fetches notifications from TMS
 */
public abstract class AbstractNotificationSubscriberService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractNotificationSubscriberService.class);
  // Sampling log only log once per 10000
  private static final Logger SAMPLING_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(10000));

  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final Transactional transactional;
  private final MultiThreadMessagingContext messagingContext;
  private final MetricsCollectionService metricsCollectionService;
  private volatile boolean stopping;

  @Inject
  protected AbstractNotificationSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                                  DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                                  MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.metricsCollectionService = metricsCollectionService;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      org.apache.tephra.RetryStrategies.retryOnConflict(20, 100)
    );
  }

  @Override
  protected void shutDown() {
    stopping = true;
  }

  /**
   * Thread that subscribes to TMS notifications on a specified topic and fetches notifications, retrying if necessary
   */
  protected abstract class AbstractSubscriberRunnable implements Runnable {

    private final String topic;
    private final int fetchSize;
    private final boolean transactionalFetch;
    private final long emptyFetchDelayMillis;
    private final RetryStrategy retryStrategy;
    private final MetricsContext metricsContext;
    private int failureCount;
    private String messageId;

    protected AbstractSubscriberRunnable(String name, String topic, long emptyFetchDelayMillis,
                                         int fetchSize, boolean transactionalFetch) {
      this.topic = topic;
      this.fetchSize = fetchSize;
      this.transactionalFetch = transactionalFetch;
      this.emptyFetchDelayMillis = emptyFetchDelayMillis;
      this.retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.notification.");
      this.metricsContext = metricsCollectionService.getContext(ImmutableMap.of(
        Constants.Metrics.Tag.COMPONENT, Constants.Service.MASTER_SERVICES,
        Constants.Metrics.Tag.INSTANCE_ID, cConf.get(Constants.MessagingSystem.CONTAINER_INSTANCE_ID, "0"),
        Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
        Constants.Metrics.Tag.TOPIC, topic,
        Constants.Metrics.Tag.CONSUMER, name
      ));
    }

    /**
     * Returns the topic subscribed to.
     */
    protected final String getTopic() {
      return topic;
    }

    /**
     * Called before consuming any messages. This method is called from a transaction.
     * Any non {@link RetryableException} thrown from this method will cause the runnable to terminate.
     *
     * @param context the {@link DatasetContext} for getting access to dataset
     * @return the initial message id (exclusive) for the subscription to start with or {@code null} to start
     *         from the beginning
     */
    @Nullable
    protected abstract String initialize(DatasetContext context) throws RetryableException;

    /**
     * Processes a set of notifications.
     *  @param context the dataset context
     * @param notifications an {@link Iterator} of {@link Notification} to be processed.
     */
    protected abstract void processNotifications(DatasetContext context, NotificationIterator notifications);

    /**
     * Persists the message id to storage. Note that this method is already executed inside a transaction.
     *
     * @param context the dataset context
     * @param lastFetchedMessageId the message id to persist
     */
    protected abstract void persistMessageId(DatasetContext context, String lastFetchedMessageId);

    @Override
    public void run() {
      // Fetch the last processed message for the topic.
      messageId = doInitialize();

      while (!stopping) {
        try {
          long sleepTime = processNotifications();
          // Don't sleep if sleepTime returned is 0
          if (!stopping && sleepTime > 0) {
            TimeUnit.MILLISECONDS.sleep(sleepTime);
          }
        } catch (InterruptedException e) {
          // sleep is interrupted, just exit without doing anything
          break;
        }
      }
    }

    /**
     * Performs initialization.
     */
    @Nullable
    private String doInitialize() {
      return Retries.supplyWithRetries(new Supplier<String>() {
        @Override
        public String get() {
          return Transactionals.execute(transactional, new TxCallable<String>() {
            @Override
            public String call(DatasetContext context) throws Exception {
              return initialize(context);
            }
          });
        }
      }, retryStrategy);
    }

    /**
     * Fetches new notifications and configures time for next fetch
     *
     * @return sleep time in milliseconds before next fetch
     */
    private long processNotifications() {
      try {
        // Collects batch of messages for processing.
        // The fetch may be transactional, and it's ok to have the fetching and the processing happen in two
        // non-overlapping transactions, as long as the processing transaction starts after the fetching one.
        Stopwatch stopwatch = new Stopwatch().start();
        final List<Message> messages = fetchMessages(messageId);
        metricsContext.gauge("tms.fetch.time.ms", stopwatch.elapsedTime(TimeUnit.MILLISECONDS));
        metricsContext.increment("tms.fetch.messages", messages.size());

        // Return if stopping or request to sleep for configured number of milliseconds if there are no notifications
        if (stopping || messages.isEmpty()) {
          return emptyFetchDelayMillis;
        }

        stopwatch.reset().start();
        // Process the notifications and record the message id of where the processing is up to.
        NotificationIterator iterator = Transactionals.execute(transactional, new TxCallable<NotificationIterator>() {
          @Override
          public NotificationIterator call(DatasetContext context) throws Exception {
            NotificationIterator iterator = new NotificationIterator(topic, messages.iterator());
            processNotifications(context, iterator);
            String lastMessageId = iterator.getLastMessageId();

            // Persist the message id of the last message being consumed from the iterator
            if (lastMessageId != null) {
              persistMessageId(context, lastMessageId);
            }
            return iterator;
          }
        });
        messageId = iterator.getLastMessageId() == null ? messageId : iterator.getLastMessageId();

        metricsContext.gauge("process.duration.ms", stopwatch.elapsedTime(TimeUnit.MILLISECONDS));
        metricsContext.increment("process.notifications", iterator.getConsumedCount());

        // Calculate the delay
        metricsContext.gauge("process.delay.ms", System.currentTimeMillis() - getMessagePublishTime(messageId));

        // Transaction was successful, so reset the failure count to 0
        failureCount = 0;
        return 0L;

      } catch (ServiceUnavailableException e) {
        SAMPLING_LOG.warn("Failed to contact service {}. Will retry in next run.", e.getServiceName(), e);
      } catch (TopicNotFoundException e) {
        SAMPLING_LOG.warn("Failed to fetch from TMS. Will retry in next run.", e);
      } catch (Exception e) {
        SAMPLING_LOG.warn("Failed to get and process notifications. Will retry in next run", e);
      }

      // If there is any failure during fetching of notifications or looking up of schedules,
      // delay the next fetch based on the strategy
      // Exponential strategy doesn't use the time component, so doesn't matter what we passed in as startTime
      return retryStrategy.nextRetry(++failureCount, 0);
    }

    /**
     * Fetch messages from TMS, optionally with transaction.
     */
    private List<Message> fetchMessages(@Nullable final String messageId) throws TopicNotFoundException, IOException {
      if (!transactionalFetch) {
        return doFetchMessages(messageId);
      }
      return Transactionals.execute(transactional, new TxCallable<List<Message>>() {
        @Override
        public List<Message> call(DatasetContext context) throws Exception {
          return doFetchMessages(messageId);
        }
      }, TopicNotFoundException.class, IOException.class);
    }

    /**
     * Actually fetching messages from TMS.
     */
    private List<Message> doFetchMessages(@Nullable String messageId) throws TopicNotFoundException, IOException {
      List<Message> messages = new ArrayList<>();
      LOG.trace("Fetching system topic '{}' with messageId '{}'", topic, messageId);
      MessageFetcher messageFetcher = messagingContext.getMessageFetcher();
      try (CloseableIterator<Message> iterator = messageFetcher.fetch(NamespaceId.SYSTEM.getNamespace(),
                                                                      topic, fetchSize, messageId)) {
        while (iterator.hasNext() && !stopping) {
          messages.add(iterator.next());
        }
      }
      return messages;
    }

    /**
     * Returns the publish time encoded in the given message id.
     *
     * @param messageId the message id to decode
     * @return the publish time or {@code 0} if the message id is {@code null}.
     */
    private long getMessagePublishTime(@Nullable String messageId) {
      if (messageId == null) {
        return 0L;
      }

      return new MessageId(Bytes.fromHexString(messageId)).getPublishTimestamp();
    }
  }

  /**
   * An {@link Iterator} that transform {@link Message} into {@link Notification} and maintain the message id that
   * it has consumed up to.
   */
  protected static final class NotificationIterator extends AbstractIterator<Notification> {

    private final String topic;
    private final Iterator<Message> messages;
    private String lastMessageId;
    private int consumedCount;

    NotificationIterator(String topic, Iterator<Message> messages) {
      this.topic = topic;
      this.messages = messages;
    }

    @Override
    protected Notification computeNext() {
      // Decode the next message into Notification.
      while (messages.hasNext()) {
        consumedCount++;
        Message message = messages.next();
        lastMessageId = message.getId();

        try {
          Notification notification = GSON.fromJson(message.getPayloadAsString(), Notification.class);
          LOG.trace("Processing notification from topic {} with message id {}: {}", topic, lastMessageId, notification);
          return notification;
        } catch (JsonSyntaxException e) {
          // This shouldn't happen.
          LOG.warn("Failed to decode message with id {} and payload '{}'. Skipped.",
                   message.getId(), message.getPayloadAsString(), e);
        }
      }
      return endOfData();
    }

    @Nullable
    String getLastMessageId() {
      return lastMessageId;
    }

    int getConsumedCount() {
      return consumedCount;
    }
  }
}
