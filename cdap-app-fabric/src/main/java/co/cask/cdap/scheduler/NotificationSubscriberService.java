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

package co.cask.cdap.scheduler;

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
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.internal.app.runtime.schedule.queue.JobQueueDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ScheduleId;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Subscribe to notification TMS topic and update schedules in schedule store and job queue
 */
class NotificationSubscriberService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationSubscriberService.class);
  // Sampling log only log once per 10000
  private static final Logger SAMPLING_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(10000));
  private static final Gson GSON = new Gson();

  private final Transactional transactional;
  private final MultiThreadMessagingContext messagingContext;
  private final DatasetFramework datasetFramework;
  private final MultiThreadDatasetCache multiThreadDatasetCache;
  private final CConfiguration cConf;
  private ExecutorService taskExecutorService;
  private volatile boolean stopping = false;


  @Inject
  NotificationSubscriberService(MessagingService messagingService,
                                CConfiguration cConf,
                                DatasetFramework datasetFramework,
                                TransactionSystemClient txClient) {
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

  @Override
  protected void startUp() {
    LOG.info("Start running NotificationSubscriberService");
    taskExecutorService =
      Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("scheduler-subscriber-task-%d").build());

    taskExecutorService.submit(new SchedulerEventNotificationSubscriberThread(
      cConf.get(Constants.Scheduler.TIME_EVENT_TOPIC)));
    taskExecutorService.submit(new SchedulerEventNotificationSubscriberThread(
      cConf.get(Constants.Scheduler.STREAM_SIZE_EVENT_TOPIC)));
    taskExecutorService.submit(new DataEventNotificationSubscriberThread());
  }

  @Override
  protected void shutDown() {
    stopping = true;
    LOG.info("Stopping NotificationSubscriberService.");
    try {
      taskExecutorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    } finally {
      if (!taskExecutorService.isTerminated()) {
        taskExecutorService.shutdownNow();
      }
    }
    LOG.info("Stopped NotificationSubscriberService.");
  }

  private abstract class NotificationSubscriberThread implements Runnable {
    private final String topic;
    private final RetryStrategy scheduleStrategy;
    private int failureCount;
    private String messageId;
    JobQueueDataset jobQueue;


    NotificationSubscriberThread(String topic) {
      this.topic = topic;
      // TODO: [CDAP-11370] Need to be configured in cdap-default.xml. Retry with delay ranging from 0.1s to 30s
      scheduleStrategy =
        co.cask.cdap.common.service.RetryStrategies.exponentialDelay(100, 30000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
      jobQueue = Schedulers.getJobQueue(multiThreadDatasetCache, datasetFramework);
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

    private String loadMessageId() {
      return Transactionals.execute(transactional, new TxCallable<String>() {
        @Override
        public String call(DatasetContext context) throws Exception {
          return jobQueue.retrieveSubscriberState(topic);
        }
      });
    }

    /**
     * Fetch new notifications and update job queue
     *
     * @return sleep time in milliseconds before next fetch
     */
    private long processNotifications() {
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

        // The job queue and the last fetched message id was persisted successfully, update the local field as well.
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
      return scheduleStrategy.nextRetry(++failureCount, 0);
    }

    @Nullable
    private String fetchAndProcessNotifications(DatasetContext context, MessageFetcher fetcher) throws Exception {
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
          updateJobQueue(context, notification);
          lastFetchedMessageId = message.getId();
        }

        if (lastFetchedMessageId != null) {
          jobQueue.persistSubscriberState(topic, lastFetchedMessageId);
        }

        return lastFetchedMessageId;
      }
    }

    abstract void updateJobQueue(DatasetContext context, Notification notification) throws Exception;
  }

  private class SchedulerEventNotificationSubscriberThread extends NotificationSubscriberThread {

    SchedulerEventNotificationSubscriberThread(String topic) {
      super(topic);
    }

    @Override
    protected void updateJobQueue(DatasetContext context, Notification notification)
      throws IOException, DatasetManagementException, NotFoundException {

      Map<String, String> properties = notification.getProperties();
      String scheduleIdString = properties.get(ProgramOptionConstants.SCHEDULE_ID);
      if (scheduleIdString == null) {
        LOG.warn("Cannot find schedule id in the notification with properties {}. Skipping current notification.",
                 properties);
        return;
      }
      ScheduleId scheduleId;
      try {
        scheduleId = GSON.fromJson(scheduleIdString, ScheduleId.class);
      } catch (JsonSyntaxException e) {
        // If the notification is from pre-4.3 version, scheduleId is not in JSON format,
        // parse it with fromString method
        scheduleId = ScheduleId.fromString(scheduleIdString);
      }
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
  }

  private class DataEventNotificationSubscriberThread extends NotificationSubscriberThread {

    DataEventNotificationSubscriberThread() {
      super(cConf.get(Constants.Dataset.DATA_EVENT_TOPIC));
    }

    @Override
    protected void updateJobQueue(DatasetContext context, Notification notification)
      throws IOException, DatasetManagementException {
      String datasetIdString = notification.getProperties().get(Notification.DATASET_ID);
      if (datasetIdString == null) {
        return;
      }
      DatasetId datasetId = DatasetId.fromString(datasetIdString);
      for (ProgramScheduleRecord schedule : getSchedules(context, Schedulers.triggerKeyForPartition(datasetId))) {
        // ignore disabled schedules
        if (ProgramScheduleStatus.SCHEDULED.equals(schedule.getMeta().getStatus())) {
          jobQueue.addNotification(schedule, notification);
        }
      }
    }
  }

  private Collection<ProgramScheduleRecord> getSchedules(DatasetContext context, String triggerKey)
    throws IOException, DatasetManagementException {
    return Schedulers.getScheduleStore(context, datasetFramework).findSchedules(triggerKey);
  }
}
