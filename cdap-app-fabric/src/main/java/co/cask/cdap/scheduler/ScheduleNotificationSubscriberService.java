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
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.workflow.NodeValue;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.internal.app.runtime.schedule.queue.JobQueueDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.internal.app.services.AbstractNotificationSubscriberService;
import co.cask.cdap.internal.app.services.ProgramNotificationSubscriberService;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.ScheduleId;
import com.google.common.collect.Collections2;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Subscribe to notification TMS topic and update schedules in schedule store and job queue
 */
public class ScheduleNotificationSubscriberService extends AbstractNotificationSubscriberService {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduleNotificationSubscriberService.class);
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final CConfiguration cConf;
  private final DatasetFramework datasetFramework;
  private ExecutorService taskExecutorService;
  private ProgramNotificationSubscriberService programNotificationSubscriberService;

  @Inject
  ScheduleNotificationSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                        DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                        ProgramNotificationSubscriberService programNotificationSubscriberService) {
    super(messagingService, cConf, datasetFramework, txClient);

    this.cConf = cConf;
    this.datasetFramework = datasetFramework;
    this.programNotificationSubscriberService = programNotificationSubscriberService;
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
    taskExecutorService.submit(new ProgramStatusEventNotificationSubscriberThread());
    programNotificationSubscriberService.startAndWait();
  }

  @Override
  protected void shutDown() {
    super.shutDown();
    try {
      taskExecutorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    } finally {
      if (!taskExecutorService.isTerminated()) {
        taskExecutorService.shutdownNow();
      }
    }
    programNotificationSubscriberService.stopAndWait();
    LOG.info("Stopped SchedulerNotificationSubscriberService.");
  }

  /**
   * Thread that subscribes to TMS notifications and adds the notification containing the schedule id to the job queue
   */
  private class SchedulerEventNotificationSubscriberThread extends NotificationSubscriberThread {
    private String topic;

    SchedulerEventNotificationSubscriberThread(String topic) {
      super(topic);
      this.topic = topic;
    }

    @Override
    public String loadMessageId(DatasetContext context) {
      return getJobQueue(context).retrieveSubscriberState(topic);
    }

    @Override
    public void updateMessageId(DatasetContext context, String lastFetchedMessageId) {
      getJobQueue(context).persistSubscriberState(topic, lastFetchedMessageId);
    }

    @Override
    public void processNotification(DatasetContext context, Notification notification)
      throws IOException, DatasetManagementException {

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
      getJobQueue(context).addNotification(record, notification);
    }

    JobQueueDataset getJobQueue(DatasetContext datasetContext) {
      return Schedulers.getJobQueue(datasetContext, datasetFramework);
    }
  }

  private class DataEventNotificationSubscriberThread extends SchedulerEventNotificationSubscriberThread {

    DataEventNotificationSubscriberThread() {
      super(cConf.get(Constants.Dataset.DATA_EVENT_TOPIC));
    }

    @Override
    public void processNotification(DatasetContext context, Notification notification)
      throws IOException, DatasetManagementException {

      String datasetIdString = notification.getProperties().get("datasetId");
      if (datasetIdString == null) {
        return;
      }
      DatasetId datasetId = DatasetId.fromString(datasetIdString);
      for (ProgramScheduleRecord schedule : getSchedules(context, Schedulers.triggerKeyForPartition(datasetId))) {
        // ignore disabled schedules
        if (ProgramScheduleStatus.SCHEDULED.equals(schedule.getMeta().getStatus())) {
          getJobQueue(context).addNotification(schedule, notification);
        }
      }
    }
  }

  /**
   * Private class that receives program status notifications forwarded from
   * {@link co.cask.cdap.internal.app.services.ProgramNotificationSubscriberService}.
   *
   * Processes notifications that are guaranteed to trigger a program
   */
  private class ProgramStatusEventNotificationSubscriberThread extends SchedulerEventNotificationSubscriberThread {
    ProgramStatusEventNotificationSubscriberThread() {
      super(cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC));
    }

    @Override
    public void processNotification(DatasetContext context, Notification notification)
      throws IOException, DatasetManagementException {

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
      String runId = programRunId.getRun();
      String triggerKeyForProgramStatus = Schedulers.triggerKeyForProgramStatus(programId, programStatus);
      Collection<ProgramScheduleRecord> triggeredSchedules = getSchedules(context, triggerKeyForProgramStatus);

      RunRecordMeta triggeringProgramRun = null;
      WorkflowToken triggeringWorkflowToken = null;

      // Only fetch the triggering program information if there is at least one triggered and enabled schedule
      Collection<ProgramScheduleRecord> enabledSchedules =
        Collections2.filter(triggeredSchedules, new com.google.common.base.Predicate<ProgramScheduleRecord>() {
          @Override
          public boolean apply(ProgramScheduleRecord programScheduleRecord) {
            return programScheduleRecord.getMeta().getStatus() == ProgramScheduleStatus.SCHEDULED;
          }
        });

      if (!enabledSchedules.isEmpty()) {
        triggeringProgramRun = getAppMetadataStore(context).getRun(programId, runId);
        triggeringWorkflowToken = getAppMetadataStore(context).getWorkflowToken(programId, runId);
      }

      for (ProgramScheduleRecord schedule : enabledSchedules) {
        Map<String, String> triggeringProgramRunProperties =
          GSON.fromJson(triggeringProgramRun.getProperties().get(ProgramOptionConstants.RUNTIME_ARGS),
                        MAP_STRING_STRING_TYPE);
        Map<String, String> triggeredProgramRunProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : triggeringProgramRunProperties.entrySet()) {
          triggeredProgramRunProperties.put(entry.getKey(), entry.getValue());
        }

        Map<String, String> triggeredProgramProperties = new HashMap<>();
        triggeredProgramProperties.put(ProgramOptionConstants.USER_OVERRIDES,
                                       GSON.toJson(triggeredProgramRunProperties));

        // If the triggered program is a workflow, send the notification that contains just the USER workflow token
        if (schedule.getSchedule().getProgramId().getType() == ProgramType.WORKFLOW &&
          programId.getType() == ProgramType.WORKFLOW) {

          // Copy over the workflow token and extract just the user scoped keys
          Map<String, List<NodeValue>> userValues = triggeringWorkflowToken.getAll(WorkflowToken.Scope.USER);

          BasicWorkflowToken userWorkflowToken = new BasicWorkflowToken(
            cConf.getInt(Constants.AppFabric.WORKFLOW_TOKEN_MAX_SIZE_MB));
          userWorkflowToken.setCurrentNode(programId.getProgram());

          String prefix = schedule.getSchedule().getProgramId().getNamespace() + "." +
            schedule.getSchedule().getProgramId().getApplication();

          for (Map.Entry<String, List<NodeValue>> entry : userValues.entrySet()) {
            for (NodeValue nodeValue : entry.getValue()) {
              userWorkflowToken.put(prefix + "." + entry.getKey(), nodeValue.getValue());
            }
          }
          triggeredProgramProperties.put(ProgramOptionConstants.WORKFLOW_TOKEN, GSON.toJson(userWorkflowToken));
        }

        // TODO send triggering program metadata? encode in system args, extract in AbstractContext
        Notification triggeredProgramNotification = new Notification(Notification.Type.PROGRAM_STATUS,
                                                                     triggeredProgramProperties);
        getJobQueue(context).addNotification(schedule, triggeredProgramNotification);
      }
    }
  }

  private Collection<ProgramScheduleRecord> getSchedules(DatasetContext context, String triggerKey)
    throws IOException, DatasetManagementException {
    return Schedulers.getScheduleStore(context, datasetFramework).findSchedules(triggerKey);
  }
}
