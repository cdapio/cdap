/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ImmutablePair;
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
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Subscribe to notification TMS topic and update schedules in schedule store and job queue
 */
public class ScheduleNotificationSubscriberService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduleNotificationSubscriberService.class);
  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final MessagingService messagingService;
  private final DatasetFramework datasetFramework;
  private final TransactionSystemClient txClient;
  private final MetricsCollectionService metricsCollectionService;
  private final List<Service> subscriberServices;
  private ScheduledExecutorService subscriberExecutor;

  @Inject
  ScheduleNotificationSubscriberService(CConfiguration cConf, MessagingService messagingService,
                                        DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                        MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    this.messagingService = messagingService;
    this.datasetFramework = datasetFramework;
    this.txClient = txClient;
    this.metricsCollectionService = metricsCollectionService;
    this.subscriberServices = Arrays.asList(new SchedulerEventSubscriberService(),
                                            new DataEventSubscriberService(),
                                            new ProgramStatusEventSubscriberService());
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {}", getClass().getSimpleName());

    // Use a shared executor for all different subscribers and only keep one core thread
    subscriberExecutor = Executors.newScheduledThreadPool(
      1, Threads.createDaemonThreadFactory("scheduler-notification-subscriber-%d"));

    // Start all subscriber services. All of them has no-op in start, so they shouldn't fail.
    Futures.successfulAsList(subscriberServices.stream().map(Service::start).collect(Collectors.toList())).get();
  }

  @Override
  protected void shutDown() throws Exception {
    // This never throw
    Futures.successfulAsList(subscriberServices.stream().map(Service::stop).collect(Collectors.toList())).get();

    for (Service service : subscriberServices) {
      // The service must have been stopped, and calling stop again will just return immediate with the
      // future that carries the stop state.
      try {
        service.stop().get();
      } catch (ExecutionException e) {
        LOG.warn("Exception raised when stopping service {}", service, e.getCause());
      }
    }

    subscriberExecutor.shutdownNow();
    LOG.info("Stopped {}", getClass().getSimpleName());
  }

  /**
   * Abstract base class for implementing job queue logic for various kind of notifications.
   */
  private abstract class AbstractSchedulerSubscriberService extends AbstractNotificationSubscriberService {

    AbstractSchedulerSubscriberService(String name, String topic, int fetchSize, boolean transactionalFetch) {
      super(name, cConf, topic, transactionalFetch, fetchSize,
            cConf.getLong(Constants.Scheduler.EVENT_POLL_DELAY_MILLIS),
            messagingService, datasetFramework, txClient, metricsCollectionService);
    }

    @Nullable
    @Override
    protected String loadMessageId(DatasetContext datasetContext) throws Exception {
      return getJobQueue(datasetContext).retrieveSubscriberState(getTopicId().getTopic());
    }

    @Override
    protected void storeMessageId(DatasetContext datasetContext, String messageId) throws Exception {
      getJobQueue(datasetContext).persistSubscriberState(getTopicId().getTopic(), messageId);
    }

    @Override
    protected void processMessages(DatasetContext datasetContext,
                                   Iterator<ImmutablePair<String, Notification>> messages) throws Exception {
      ProgramScheduleStoreDataset scheduleStore = getScheduleStore(datasetContext);
      JobQueueDataset jobQueue = getJobQueue(datasetContext);

      while (messages.hasNext()) {
        processNotification(scheduleStore, jobQueue, messages.next().getSecond());
      }
    }

    @Override
    protected ScheduledExecutorService executor() {
      return subscriberExecutor;
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
  private final class SchedulerEventSubscriberService extends AbstractSchedulerSubscriberService {

    SchedulerEventSubscriberService() {
      // Time and stream size events are non-transactional
      super("scheduler.event", cConf.get(Constants.Scheduler.TIME_EVENT_TOPIC),
            cConf.getInt(Constants.Scheduler.TIME_EVENT_FETCH_SIZE), false);
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
  private final class DataEventSubscriberService extends AbstractSchedulerSubscriberService {

    DataEventSubscriberService() {
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
  private final class ProgramStatusEventSubscriberService extends AbstractSchedulerSubscriberService {

    ProgramStatusEventSubscriberService() {
      // Fetch transactionally since publishing from AppMetadataStore is transactional.
      super("scheduler.program.event", cConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC),
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
