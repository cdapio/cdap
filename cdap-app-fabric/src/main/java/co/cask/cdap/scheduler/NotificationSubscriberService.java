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

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.store.Store;
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
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.messaging.MultiThreadMessagingContext;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.internal.app.runtime.schedule.queue.JobQueueDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Subscribe to notification TMS topic and update schedules in schedule store and job queue
 */
class NotificationSubscriberService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationSubscriberService.class);
  // Sampling log only log once per 10000
  private static final Logger SAMPLING_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(10000));
  private static final Gson GSON = new Gson();
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();

  private final Transactional transactional;
  private final MultiThreadMessagingContext messagingContext;
  private final DatasetFramework datasetFramework;
  private final MultiThreadDatasetCache multiThreadDatasetCache;
  private final CConfiguration cConf;
  private final Store store;
  private ListeningExecutorService taskExecutorService;
  private volatile boolean stopping = false;


  @Inject
  NotificationSubscriberService(MessagingService messagingService,
                                CConfiguration cConf,
                                DatasetFramework datasetFramework,
                                TransactionSystemClient txClient,
                                Store store) {
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
    this.store = store;
  }

  @Override
  protected void startUp() {
    LOG.info("Start running NotificationSubscriberService");
    taskExecutorService = MoreExecutors.listeningDecorator(
      Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("scheduler-subscriber-task").build()));

    taskExecutorService.submit(new SchedulerEventNotificationSubscriberThread(
      cConf.get(Constants.Scheduler.TIME_EVENT_TOPIC)));
    taskExecutorService.submit(new SchedulerEventNotificationSubscriberThread(
      cConf.get(Constants.Scheduler.STREAM_SIZE_EVENT_TOPIC)));
    taskExecutorService.submit(new DataEventNotificationSubscriberThread());
    taskExecutorService.submit(new ProgramStatusEventNotificationSubscriberThread(
      cConf.get(Constants.Scheduler.PROGRAM_STATUS_EVENT_TOPIC)));
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
      try {
        return Transactions.execute(transactional, new TxCallable<String>() {
          @Override
          public String call(DatasetContext context) throws Exception {
            return jobQueue.retrieveSubscriberState(topic);
          }
        });
      } catch (TransactionFailureException e) {
        // if we fail to load the messageId, we want to propagate the exception, so we don't reprocess
        // TODO: retry? What's the recovery strategy?
        throw Throwables.propagate(e);
      }
    }

    /**
     * Fetch new notifications and update job queue
     *
     * @return sleep time in milliseconds before next fetch
     */
    private long processNotifications() {
      boolean emptyFetch = false;
      try {
        final MessageFetcher fetcher = messagingContext.getMessageFetcher();
        emptyFetch = Transactions.execute(transactional, new TxCallable<Boolean>() {
          @Override
          public Boolean call(DatasetContext context) throws Exception {
            return fetchAndProcessNotifications(context, fetcher);
          }
        });
        failureCount = 0;
      } catch (Exception e) {
        LOG.warn("Failed to get and process notifications. Will retry in next run", e);
        failureCount++;
      }

      // If there is any failure during fetching of notifications or looking up of schedules,
      // delay the next fetch based on the strategy
      if (failureCount > 0) {
        // Exponential strategy doesn't use the time component, so doesn't matter what we passed in as startTime
        return scheduleStrategy.nextRetry(failureCount, 0);
      }

      // Sleep for 2 seconds if there's no notification, otherwise don't sleep
      return emptyFetch ? 2000L : 0L;
    }

    private boolean fetchAndProcessNotifications(DatasetContext context, MessageFetcher fetcher) throws Exception {
      boolean emptyFetch = true;
      try (CloseableIterator<Message> iterator = fetcher.fetch(NamespaceId.SYSTEM.getNamespace(),
                                                               topic, 100, messageId)) {
        LOG.trace("Fetch with messageId = {}", messageId);
        while (iterator.hasNext() && !stopping) {
          emptyFetch = false;
          Message message = iterator.next();
          Notification notification;
          try {
            notification = GSON.fromJson(new String(message.getPayload(), StandardCharsets.UTF_8),
                                         Notification.class);
          } catch (JsonSyntaxException e) {
            LOG.warn("Failed to decode message with id {}. Skipped. ", message.getId(), e);
            messageId = message.getId(); // update messageId to skip this message in next fetch
            continue;
          }
          updateJobQueue(context, notification);
          messageId = message.getId();
        }

        if (!emptyFetch) {
          jobQueue.persistSubscriberState(topic, messageId);
        }
      } catch (ServiceUnavailableException | TopicNotFoundException e) {
        SAMPLING_LOG.warn("Failed to fetch from TMS. Will retry later.", e);
        failureCount++;
      }
      return emptyFetch;
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
  }

  private class DataEventNotificationSubscriberThread extends NotificationSubscriberThread {

    DataEventNotificationSubscriberThread() {
      super(cConf.get(Constants.Dataset.DATA_EVENT_TOPIC));
    }

    @Override
    protected void updateJobQueue(DatasetContext context, Notification notification)
      throws IOException, DatasetManagementException {
      String datasetIdString = notification.getProperties().get("datasetId");
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

  private class ProgramStatusEventNotificationSubscriberThread extends NotificationSubscriberThread {

    ProgramStatusEventNotificationSubscriberThread(String topic) {
      super(topic);
    }

    @Override
    protected void updateJobQueue(DatasetContext context, Notification notification)
      throws IOException, DatasetManagementException, NotFoundException {

      String programIdString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_ID);
      String programRunId = notification.getProperties().get(ProgramOptionConstants.RUN_ID);
      String userOverridesString = notification.getProperties().get(ProgramOptionConstants.USER_OVERRIDES);
      String programStatusString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_STATUS);
      ProgramStatus programStatus = ProgramStatus.valueOf(programStatusString);

      if (programIdString == null || programRunId == null) {
        return;
      }

      Map<String, String> properties = new HashMap<>();
      properties.put(ProgramOptionConstants.USER_OVERRIDES, userOverridesString);

      // Add workflow token if triggering program was a Workflow
      ProgramId programId = ProgramId.fromString(programIdString);
      if (programId.getType() == ProgramType.WORKFLOW) {
        WorkflowId workflowId = programId.getParent().workflow(programId.getProgram());
        WorkflowToken token = store.getWorkflowToken(workflowId, programRunId);

        Map<String, String> workflowInfoProvider = new HashMap<>();
        workflowInfoProvider.put(ProgramOptionConstants.WORKFLOW_NAME, workflowId.getProgram());
        workflowInfoProvider.put(ProgramOptionConstants.WORKFLOW_RUN_ID, programRunId);
        workflowInfoProvider.put(ProgramOptionConstants.WORKFLOW_NODE_ID, "doesn't matter?");
        workflowInfoProvider.put(ProgramOptionConstants.PROGRAM_NAME_IN_WORKFLOW, "doesn't matter?");
        workflowInfoProvider.put(ProgramOptionConstants.WORKFLOW_TOKEN, GSON.toJson(token));
        properties.put(ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(workflowInfoProvider, STRING_STRING_MAP));
      }
      Notification workflowNotification = new Notification(Notification.Type.PROGRAM_STATUS, properties);

      String triggerKeyForProgramStatus = Schedulers.triggerKeyForProgramStatus(programId, programStatus);

      for (ProgramScheduleRecord schedule : getSchedules(context, triggerKeyForProgramStatus)) {
        if (schedule.getMeta().getStatus() == ProgramScheduleStatus.SCHEDULED) {
          // If the triggered program is a workflow, send the notification that contains the workflow token to be used
          if (schedule.getSchedule().getProgramId().getType() == ProgramType.WORKFLOW) {
            jobQueue.addNotification(schedule, workflowNotification);
          } else {
            // Send the original notification
            jobQueue.addNotification(schedule, notification);
          }
        }
      }
    }
  }

  private Collection<ProgramScheduleRecord> getSchedules(DatasetContext context, String triggerKey)
    throws IOException, DatasetManagementException {
    return Schedulers.getScheduleStore(context, datasetFramework).findSchedules(triggerKey);
  }
}
