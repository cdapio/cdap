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

import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.TxCallable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.internal.app.runtime.schedule.queue.JobQueueDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.app.services.AbstractNotificationSubscriberService;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ScheduleId;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Subscribe to notification TMS topic and update schedules in schedule store and job queue
 */
class ScheduleNotificationSubscriberService extends AbstractNotificationSubscriberService {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduleNotificationSubscriberService.class);

  @Inject
  ScheduleNotificationSubscriberService(MessagingService messagingService, Store store,
                                        CConfiguration cConf, DatasetFramework datasetFramework,
                                        TransactionSystemClient txClient) {
    super(messagingService, store, cConf, datasetFramework, txClient);
  }

  @Override
  protected void startUp() {
    LOG.info("Start running ScheduleNotificationSubscriberService");

    taskExecutorService =
      Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("scheduler-subscriber-task-%d").build());
    taskExecutorService.submit(new SchedulerEventNotificationSubscriberThread(
      cConf.get(Constants.Scheduler.TIME_EVENT_TOPIC)));
    taskExecutorService.submit(new SchedulerEventNotificationSubscriberThread(
      cConf.get(Constants.Scheduler.STREAM_SIZE_EVENT_TOPIC)));
    taskExecutorService.submit(new DataEventNotificationSubscriberThread());
  }

  private class SchedulerEventNotificationSubscriberThread
      extends AbstractNotificationSubscriberService.NotificationSubscriberThread {
    JobQueueDataset jobQueue;
    private String topic;

    SchedulerEventNotificationSubscriberThread(String topic) {
      super(topic);
      this.topic = topic;
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
      String lastFetchedMessageId = super.fetchAndProcessNotifications(context, fetcher);
      if (lastFetchedMessageId != null) {
        jobQueue.persistSubscriberState(topic, lastFetchedMessageId);
      }
      return lastFetchedMessageId;
    }

    @Override
    protected void processNotification(DatasetContext context, Notification notification)
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

  private class DataEventNotificationSubscriberThread extends SchedulerEventNotificationSubscriberThread {

    DataEventNotificationSubscriberThread() {
      super(cConf.get(Constants.Dataset.DATA_EVENT_TOPIC));
    }

    @Override
    protected void processNotification(DatasetContext context, Notification notification)
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

  private Collection<ProgramScheduleRecord> getSchedules(DatasetContext context, String triggerKey)
    throws IOException, DatasetManagementException {
    return Schedulers.getScheduleStore(context, datasetFramework).findSchedules(triggerKey);
  }
}
