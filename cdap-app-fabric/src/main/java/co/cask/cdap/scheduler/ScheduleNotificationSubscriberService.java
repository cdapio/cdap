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
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.ScheduleId;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

  private final CConfiguration cConf;
  private final DatasetFramework datasetFramework;
  private ExecutorService taskExecutorService;

  @Inject
  ScheduleNotificationSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                        DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    super(messagingService, cConf, datasetFramework, txClient);

    this.cConf = cConf;
    this.datasetFramework = datasetFramework;
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
      ScheduleId scheduleId = ScheduleId.fromString(scheduleIdString);
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
      super(cConf.get(Constants.Scheduler.PROGRAM_STATUS_EVENT_TOPIC));
    }

    @Override
    public void processNotification(DatasetContext context, Notification notification)
      throws IOException, DatasetManagementException {

      String programRunIdString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_RUN_ID);
      String programRunStatusString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_STATUS);

      ProgramStatus programStatus = null;
      try {
        programStatus = ProgramRunStatus.toProgramStatus(ProgramRunStatus.valueOf(programRunStatusString));
      } catch (IllegalArgumentException e) {
        LOG.warn("Invalid program status {} passed for programId {}", programRunStatusString, programRunIdString, e);
        // Fall through, let the thread return normally
      }

      // Ignore notifications which specify an invalid ProgramId, RunId, or ProgramStatus
      if (programRunIdString == null || programStatus == null) {
        return;
      }

      ProgramRunId programRunId = GSON.fromJson(programRunIdString, ProgramRunId.class);
      ProgramId programId = programRunId.getParent();
      String runId = programRunId.getRun();
      String triggerKeyForProgramStatus = Schedulers.triggerKeyForProgramStatus(programId, programStatus);

      for (ProgramScheduleRecord schedule : getSchedules(context, triggerKeyForProgramStatus)) {
        if (schedule.getMeta().getStatus() == ProgramScheduleStatus.SCHEDULED) {
          // Copy over the user properties
          RunRecordMeta triggeredProgramRun = getAppMetadataStore(context).getRun(programId, runId);
          Map<String, String> triggeringProgramProperties = new HashMap<>();
          if (triggeredProgramRun.getProperties() != null) {
            triggeringProgramProperties.put(ProgramOptionConstants.USER_OVERRIDES,
                                            GSON.toJson(triggeredProgramRun.getProperties()));
          }

          // If the triggered program is a workflow, send the notification that contains just the USER workflow token
          if (schedule.getSchedule().getProgramId().getType() == ProgramType.WORKFLOW &&
            programId.getType() == ProgramType.WORKFLOW) {

            // Copy over the workflow token and extract just the user scoped keys
            WorkflowToken workflowToken = getAppMetadataStore(context).getWorkflowToken(programId, runId);
            Map<String, List<NodeValue>> userValues = workflowToken.getAll(WorkflowToken.Scope.USER);

            BasicWorkflowToken userWorkflowToken = new BasicWorkflowToken(
              cConf.getInt(Constants.AppFabric.WORKFLOW_TOKEN_MAX_SIZE_MB));
            userWorkflowToken.setCurrentNode(programId.getProgram());

            for (Map.Entry<String, List<NodeValue>> entry : userValues.entrySet()) {
              for (NodeValue nodeValue : entry.getValue()) {
                userWorkflowToken.put(entry.getKey(), nodeValue.getValue());
              }
            }
            triggeringProgramProperties.put(ProgramOptionConstants.WORKFLOW_TOKEN, GSON.toJson(userWorkflowToken));
          }

          Notification triggeringProgram = new Notification(Notification.Type.PROGRAM_STATUS,
                                                            triggeringProgramProperties);
          getJobQueue(context).addNotification(schedule, triggeringProgram);
        }
      }
    }
  }

  private Collection<ProgramScheduleRecord> getSchedules(DatasetContext context, String triggerKey)
    throws IOException, DatasetManagementException {
    return Schedulers.getScheduleStore(context, datasetFramework).findSchedules(triggerKey);
  }
}
