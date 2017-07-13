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
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.messaging.MultiThreadMessagingContext;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.queue.JobQueueDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ScheduleId;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
  private final DatasetFramework datasetFramework;
  private final MultiThreadMessagingContext messagingContext;
  private final MultiThreadDatasetCache multiThreadDatasetCache;
  private volatile boolean stopping = false;

  @Inject
  protected AbstractNotificationSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                                  DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.cConf = cConf;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.multiThreadDatasetCache = new MultiThreadDatasetCache(
      new SystemDatasetInstantiator(datasetFramework), txClient,
      NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null, messagingContext);
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(multiThreadDatasetCache, Schedulers.SUBSCRIBER_TX_TIMEOUT_SECONDS),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.datasetFramework = datasetFramework;
  }

  protected abstract void startUp();

  @Override
  protected void shutDown() {
    LOG.info("Stopping AbstractNotificationSubscriberService.");
    stopping = true;
  }

  /**
   * Thread that subscribes to TMS notifications on a specified topic and fetches notifications, retrying if necessary
   */
  abstract class NotificationSubscriberThread implements Runnable {
    private final String topic;
    private final RetryStrategy retryStrategy;
    private int failureCount;
    private String messageId;

    NotificationSubscriberThread(String topic) {
      this.topic = topic;
      // TODO: [CDAP-11370] Need to be configured in cdap-default.xml. Retry with delay ranging from 0.1s to 30s
      retryStrategy =
        co.cask.cdap.common.service.RetryStrategies.exponentialDelay(100, 30000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
      messageId = loadMessageId();
      while (!stopping) {
        try {
          long sleepTime = processNotifications();
          // Don't sleep if sleepTime returned is 0
          if (sleepTime > 0) {
            TimeUnit.MILLISECONDS.sleep(sleepTime);
          }
        } catch (InterruptedException e) {
          // sleep is interrupted, just exit without doing anything
        }
      }
    }

    /**
     * Loads the message id from storage
     *
     * @return the message id, or null if no message id is stored
     */
    public abstract String loadMessageId();

    /**
     * Fetches new notifications and configures time for next fetch
     *
     * @return sleep time in milliseconds before next fetch
     */
    public long processNotifications() {
      try {
        final MessageFetcher fetcher = messagingContext.getMessageFetcher();
        String lastFetchedMessageId = Transactionals.execute(transactional, new TxCallable<String>() {
          @Override
          public String call(DatasetContext context) throws Exception {
            return fetchAndProcessNotifications(context, fetcher);
          }
        }, ServiceUnavailableException.class, TopicNotFoundException.class);
        failureCount = 0;

        // Sleep for configured number of milliseconds if there's no notification, otherwise don't sleep
        if (lastFetchedMessageId == null) {
          return cConf.getLong(Constants.Scheduler.EVENT_POLL_DELAY_MILLIS);
        }

        // The last fetched message id was persisted successfully, update the local field as well.
        messageId = lastFetchedMessageId;
        return 0L;

      } catch (ServiceUnavailableException e) {
        SAMPLING_LOG.warn("Failed to contact service {}. Will retry in next run.", e.getServiceName(), e);
      } catch (TopicNotFoundException e) {
        SAMPLING_LOG.warn("Failed to fetch from TMS. Will retry in next run.", e);
      } catch (Exception e) {
        LOG.warn("Failed to get and process notifications. Will retry in next run", e);
      }

      // If there is any failure during fetching of notifications or looking up of schedules,
      // delay the next fetch based on the strategy
      // Exponential strategy doesn't use the time component, so doesn't matter what we passed in as startTime
      return retryStrategy.nextRetry(++failureCount, 0);
    }

    /**
     * Fetches and decodes new notifications and processes them
     *
     * @param context the dataset context
     * @param fetcher the message fetcher
     * @return the last fetched message id
     * @throws Exception
     */
    public String fetchAndProcessNotifications(DatasetContext context, MessageFetcher fetcher) throws Exception {
      String lastFetchedMessageId = null;
      try (CloseableIterator<Message> iterator = fetcher.fetch(NamespaceId.SYSTEM.getNamespace(),
                                                               topic, 100, messageId)) {
        LOG.trace("Fetch with messageId = {}", messageId);
        while (iterator.hasNext() && !stopping) {
          Message message = iterator.next();
          Notification notification;
          try {
            notification = GSON.fromJson(new String(message.getPayload(), StandardCharsets.UTF_8),
                                         Notification.class);
          } catch (JsonSyntaxException e) {
            LOG.warn("Failed to decode message with id {}. Skipped. ", message.getId(), e);
            lastFetchedMessageId = message.getId(); // update messageId to skip this message in next fetch
            continue;
          }
          processNotification(context, notification);
          lastFetchedMessageId = message.getId();
        }
        return lastFetchedMessageId;
      }
    }

    /**
     * Processes the notification
     *
     * @param context the dataset context
     * @param notification the decoded notification
     * @throws Exception
     */
    public abstract void processNotification(DatasetContext context, Notification notification) throws Exception;
  }

  /**
   * Thread that subscribes to TMS notifications and adds the notification containing the schedule id to the job queue
   */
  protected class SchedulerEventNotificationSubscriberThread extends NotificationSubscriberThread {
    private JobQueueDataset jobQueue;
    private String topic;

    public SchedulerEventNotificationSubscriberThread(String topic) {
      super(topic);
      this.topic = topic;
    }

    @Override
    public void run() {
      jobQueue = Schedulers.getJobQueue(multiThreadDatasetCache, datasetFramework);
      super.run();
    }

    @Override
    public String loadMessageId() {
      return Transactionals.execute(transactional, new TxCallable<String>() {
        @Override
        public String call(DatasetContext context) throws Exception {
          return jobQueue.retrieveSubscriberState(topic);
        }
      });
    }

    @Override
    public String fetchAndProcessNotifications(DatasetContext context, MessageFetcher fetcher) throws Exception {
      // Get the last fetched messageId
      String lastFetchedMessageId = super.fetchAndProcessNotifications(context, fetcher);
      // Persist the the last message id for the topic to the job queue
      if (lastFetchedMessageId != null) {
        jobQueue.persistSubscriberState(topic, lastFetchedMessageId);
      }
      return lastFetchedMessageId;
    }

    @Override
    public void processNotification(DatasetContext context, Notification notification)
      throws IOException, DatasetManagementException, NotFoundException {

      Map<String, String> properties = notification.getProperties();
      String scheduleIdString = properties.get(ProgramOptionConstants.SCHEDULE_ID);
      if (scheduleIdString == null) {
        LOG.warn("Cannot find schedule id in the notification with properties {}. Skipping current notification.",
                 properties);
        return;
      }
      ScheduleId scheduleId = ScheduleId.fromString(scheduleIdString);
      ProgramScheduleRecord record;
      try {
        record = Schedulers.getScheduleStore(context, datasetFramework).getScheduleRecord(scheduleId);
      } catch (NotFoundException e) {
        LOG.warn("Cannot find schedule {}. Skipping current notification with properties {}.",
                 scheduleId, properties, e);
        return;
      }
      jobQueue.addNotification(record, notification);
    }

    public JobQueueDataset getJobQueue() {
      return jobQueue;
    }
  }
}
