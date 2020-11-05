/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.scheduler;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import io.cdap.cdap.internal.app.runtime.schedule.queue.JobQueueTable;
import io.cdap.cdap.internal.app.runtime.schedule.store.ProgramScheduleStoreDataset;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.app.services.AbstractNotificationSubscriberService;
import io.cdap.cdap.internal.capability.CapabilityManager;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  private final MetricsCollectionService metricsCollectionService;
  private final List<Service> subscriberServices;
  private final CapabilityManager capabilityManager;
  private ScheduledExecutorService subscriberExecutor;

  @Inject
  ScheduleNotificationSubscriberService(CConfiguration cConf, MessagingService messagingService,
                                        MetricsCollectionService metricsCollectionService,
                                        TransactionRunner transactionRunner, CapabilityManager capabilityManager) {
    this.cConf = cConf;
    this.messagingService = messagingService;
    this.metricsCollectionService = metricsCollectionService;
    this.subscriberServices = Arrays.asList(new SchedulerEventSubscriberService(transactionRunner),
                                            new DataEventSubscriberService(transactionRunner),
                                            new ProgramStatusEventSubscriberService(transactionRunner));
    this.capabilityManager = capabilityManager;
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
   * No transactions should be started in any of the overrided methods since they are already wrapped in a transaction.
   */
  private abstract class AbstractSchedulerSubscriberService extends AbstractNotificationSubscriberService {

    AbstractSchedulerSubscriberService(String name, String topic, int fetchSize,
                                       TransactionRunner transactionRunner) {
      super(name, cConf, topic, fetchSize, cConf.getLong(Constants.Scheduler.EVENT_POLL_DELAY_MILLIS),
            messagingService, metricsCollectionService, transactionRunner);
    }

    @Nullable
    @Override
    protected String loadMessageId(StructuredTableContext context) throws IOException {
      return getJobQueue(context).retrieveSubscriberState(getTopicId().getTopic());
    }

    @Override
    protected void storeMessageId(StructuredTableContext context, String messageId) throws IOException {
      getJobQueue(context).persistSubscriberState(getTopicId().getTopic(), messageId);
    }

    @Override
    protected void processMessages(StructuredTableContext structuredTableContext,
                                   Iterator<ImmutablePair<String, Notification>> messages) throws IOException {
      ProgramScheduleStoreDataset scheduleStore = getScheduleStore(structuredTableContext);
      JobQueueTable jobQueue = getJobQueue(structuredTableContext);

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
                                                JobQueueTable jobQueue, Notification notification) throws IOException;

    private JobQueueTable getJobQueue(StructuredTableContext context) {
      return JobQueueTable.getJobQueue(context, cConf);
    }

    private ProgramScheduleStoreDataset getScheduleStore(StructuredTableContext context) {
      return Schedulers.getScheduleStore(context);
    }
  }

  /**
   * Class responsible for time and stream size events.
   */
  private final class SchedulerEventSubscriberService extends AbstractSchedulerSubscriberService {

    SchedulerEventSubscriberService(TransactionRunner transactionRunner) {
      // Time and stream size events are non-transactional
      super("scheduler.event", cConf.get(Constants.Scheduler.TIME_EVENT_TOPIC),
            cConf.getInt(Constants.Scheduler.TIME_EVENT_FETCH_SIZE), transactionRunner);
    }

    @Override
    protected void processNotification(ProgramScheduleStoreDataset scheduleStore,
                                       JobQueueTable jobQueue, Notification notification) throws IOException {

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

      if (!capabilityManager.isApplicationEnabled(scheduleId.getNamespace(), scheduleId.getApplication())) {
        LOG.debug("Application {}.{} is not enabled, ignoring the schedule {}.", scheduleId.getNamespace(),
                  scheduleId.getApplication(), scheduleId.getSchedule());
        return;
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

    DataEventSubscriberService(TransactionRunner transactionRunner) {
      // Dataset partition events are published transactionally, hence fetch need to be transactional too.
      super("scheduler.data.event", cConf.get(Constants.Dataset.DATA_EVENT_TOPIC),
            cConf.getInt(Constants.Scheduler.DATA_EVENT_FETCH_SIZE), transactionRunner);
    }

    @Override
    protected void processNotification(ProgramScheduleStoreDataset scheduleStore,
                                       JobQueueTable jobQueue, Notification notification) throws IOException {
      String datasetIdString = notification.getProperties().get(Notification.DATASET_ID);
      if (datasetIdString == null) {
        return;
      }
      DatasetId datasetId = DatasetId.fromString(datasetIdString);
      for (ProgramScheduleRecord schedule :
        scheduleStore.findSchedules(Schedulers.triggerKeyForPartition(datasetId))) {
        jobQueue.addNotification(schedule, notification);
      }
    }
  }

  /**
   * Class responsible for program status events.
   */
  private final class ProgramStatusEventSubscriberService extends AbstractSchedulerSubscriberService {

    ProgramStatusEventSubscriberService(TransactionRunner transactionRunner) {
      // Fetch transactionally since publishing from AppMetadataStore is transactional.
      super("scheduler.program.event", cConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC),
            cConf.getInt(Constants.Scheduler.PROGRAM_STATUS_EVENT_FETCH_SIZE), transactionRunner);
    }

    @Override
    protected void processNotification(ProgramScheduleStoreDataset scheduleStore,
                                       JobQueueTable jobQueue, Notification notification) throws IOException {
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
      if (!capabilityManager
        .isApplicationEnabled(programRunId.getNamespaceId().getNamespace(), programRunId.getApplication())) {
        LOG.debug("Application {}.{} is not enabled, ignoring the schedule for program {}.",
                  programRunId.getNamespaceId().getNamespace(),
                  programRunId.getApplication(), programRunId.getProgram());
        return;
      }

      ProgramId programId = programRunId.getParent();
      String triggerKeyForProgramStatus = Schedulers.triggerKeyForProgramStatus(programId, programStatus);

      for (ProgramScheduleRecord schedule : scheduleStore.findSchedules(triggerKeyForProgramStatus)) {
        jobQueue.addNotification(schedule, notification);
      }
    }
  }
}
