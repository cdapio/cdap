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
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.retry.RetryableException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.queue.JobQueueDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.ProgramScheduleStoreDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.app.services.AbstractNotificationSubscriberService;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.ScheduleId;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Subscribe to notification TMS topic and update schedules in schedule store and job queue
 */
public class ScheduleNotificationSubscriberService extends AbstractNotificationSubscriberService {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduleNotificationSubscriberService.class);
  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final DatasetFramework datasetFramework;
  private ExecutorService taskExecutorService;

  @Inject
  ScheduleNotificationSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                        DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                        MetricsCollectionService metricsCollectionService) {
    super(messagingService, cConf, datasetFramework, txClient, metricsCollectionService);

    this.cConf = cConf;
    this.datasetFramework = datasetFramework;
  }

  @Override
  protected void startUp() {
    LOG.info("Starting {}", getClass().getSimpleName());
    taskExecutorService =
      Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("scheduler-subscriber-task-%d").build());
    taskExecutorService.submit(new SchedulerEventSubscriberRunnable(
      cConf.get(Constants.Scheduler.TIME_EVENT_TOPIC),
      cConf.getInt(Constants.Scheduler.TIME_EVENT_FETCH_SIZE)));
    taskExecutorService.submit(new SchedulerEventSubscriberRunnable(
      cConf.get(Constants.Scheduler.STREAM_SIZE_EVENT_TOPIC),
      cConf.getInt(Constants.Scheduler.STREAM_SIZE_EVENT_FETCH_SIZE)));
    taskExecutorService.submit(new DataEventSubscriberRunnable());
    taskExecutorService.submit(new ProgramStatusEventSubscriberRunnable());
  }

  @Override
  protected void shutDown() {
    super.shutDown();
    try {
      taskExecutorService.shutdownNow();
      taskExecutorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    } finally {
      if (!taskExecutorService.isTerminated()) {
        taskExecutorService.shutdownNow();
      }
    }
    LOG.info("Stopped {}", getClass().getSimpleName());
  }

  /**
   * Abstract base class for implementing job queue logic for various kind of notifications.
   */
  private abstract class AbstractSchedulerSubscriberRunnable extends AbstractSubscriberRunnable {

    AbstractSchedulerSubscriberRunnable(String name, String topic, int fetchSize, boolean transactionalFetch) {
      super(name, topic, cConf.getLong(Constants.Scheduler.EVENT_POLL_DELAY_MILLIS), fetchSize, transactionalFetch);
    }

    @Nullable
    @Override
    protected final String initialize(DatasetContext context) throws RetryableException {
      return getJobQueue(context).retrieveSubscriberState(getTopic());
    }

    @Override
    protected final void persistMessageId(DatasetContext context, String lastFetchedMessageId) {
      getJobQueue(context).persistSubscriberState(getTopic(), lastFetchedMessageId);
    }

    @Override
    protected final void processNotifications(
      DatasetContext context, AbstractNotificationSubscriberService.NotificationIterator notifications) {
      ProgramScheduleStoreDataset scheduleStore = getScheduleStore(context);
      JobQueueDataset jobQueue = getJobQueue(context);

      while (notifications.hasNext()) {
        processNotification(scheduleStore, jobQueue, notifications.next());
      }
    }

    /**
     * Processes a single {@link Notification}.
     */
    protected abstract void processNotification(ProgramScheduleStoreDataset scheduleStore,
                                                JobQueueDataset jobQueue, Notification notification);

    private JobQueueDataset getJobQueue(DatasetContext datasetContext) {
      return Schedulers.getJobQueue(datasetContext, datasetFramework);
    }

    private ProgramScheduleStoreDataset getScheduleStore(DatasetContext datasetContext) {
      return Schedulers.getScheduleStore(datasetContext, datasetFramework);
    }
  }

  /**
   * Class responsible for time and stream size events.
   */
  private final class SchedulerEventSubscriberRunnable extends AbstractSchedulerSubscriberRunnable {

    SchedulerEventSubscriberRunnable(String topic, int fetchSize) {
      // Time and stream size events are non-transactional
      super("scheduler.event", topic, fetchSize, false);
    }

    @Override
    protected void processNotification(ProgramScheduleStoreDataset scheduleStore,
                                       JobQueueDataset jobQueue, Notification notification) {

      Map<String, String> properties = notification.getProperties();
      String scheduleIdString = properties.get(ProgramOptionConstants.SCHEDULE_ID);
      if (scheduleIdString == null) {
        LOG.warn("Ignore notification that misses schedule id, {}", notification);
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
        record = scheduleStore.getScheduleRecord(scheduleId);
      } catch (NotFoundException e) {
        LOG.warn("Ignore notification that doesn't have a schedule {} associated with, {}", scheduleId, notification);
        return;
      }
      jobQueue.addNotification(record, notification);
    }
  }

  /**
   * Class responsible for dataset partition events.
   */
  private final class DataEventSubscriberRunnable extends AbstractSchedulerSubscriberRunnable {

    DataEventSubscriberRunnable() {
      // Dataset partition events are published transactionally, hence fetch need to be transactional too.
      super("scheduler.data.event", cConf.get(Constants.Dataset.DATA_EVENT_TOPIC),
            cConf.getInt(Constants.Scheduler.DATA_EVENT_FETCH_SIZE), true);
    }

    @Override
    protected void processNotification(ProgramScheduleStoreDataset scheduleStore,
                                       JobQueueDataset jobQueue, Notification notification) {
      String datasetIdString = notification.getProperties().get(Notification.DATASET_ID);
      if (datasetIdString == null) {
        return;
      }
      DatasetId datasetId = DatasetId.fromString(datasetIdString);
      for (ProgramScheduleRecord schedule : scheduleStore.findSchedules(Schedulers.triggerKeyForPartition(datasetId))) {
        jobQueue.addNotification(schedule, notification);
      }
    }
  }

  /**
   * Class responsible for program status events.
   */
  private final class ProgramStatusEventSubscriberRunnable extends AbstractSchedulerSubscriberRunnable {

    ProgramStatusEventSubscriberRunnable() {
      // Currently program status are published non-transactionally
      // However, after refactoring to have it publish from AppMetaStore update, then it should be transactional.
      // Hence for future proof, just fetch transactionally.
      super("scheduler.program.event", cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC),
            cConf.getInt(Constants.Scheduler.PROGRAM_STATUS_EVENT_FETCH_SIZE), true);
    }

    @Override
    protected void processNotification(ProgramScheduleStoreDataset scheduleStore,
                                       JobQueueDataset jobQueue, Notification notification) {
      String programRunIdString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_RUN_ID);
      String programRunStatusString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_STATUS);

      ProgramStatus programStatus;
      try {
        programStatus = ProgramRunStatus.toProgramStatus(ProgramRunStatus.valueOf(programRunStatusString));
      } catch (IllegalArgumentException e) {
        // Return silently, this happens for statuses that are not meant to be scheduled
        return;
      }

      // Ignore notifications which specify an invalid programRunId or programStatus
      if (programRunIdString == null || programStatus == null) {
        return;
      }

      ProgramRunId programRunId = GSON.fromJson(programRunIdString, ProgramRunId.class);
      ProgramId programId = programRunId.getParent();
      String triggerKeyForProgramStatus = Schedulers.triggerKeyForProgramStatus(programId, programStatus);

      for (ProgramScheduleRecord schedule : scheduleStore.findSchedules(triggerKeyForProgramStatus)) {
        jobQueue.addNotification(schedule, notification);
      }
    }
  }
}
