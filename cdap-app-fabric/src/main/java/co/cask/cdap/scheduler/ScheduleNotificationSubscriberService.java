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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.app.services.AbstractNotificationSubscriberService;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
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
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();

  private final CConfiguration cConf;
  private final DatasetFramework datasetFramework;
  private final ExecutorService taskExecutorService;

  @Inject
  ScheduleNotificationSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                        DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    super(messagingService, cConf, datasetFramework, txClient);
    this.cConf = cConf;
    this.datasetFramework = datasetFramework;
    this.taskExecutorService =
      Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("scheduler-subscriber-task-%d").build());
  }

  @Override
  protected void startUp() {
    LOG.info("Start running ScheduleNotificationSubscriberService");
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
          getJobQueue().addNotification(schedule, notification);
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
    ProgramStatusEventNotificationSubscriberThread(String topic) {
      super(topic);
    }

    @Override
    public void processNotification(DatasetContext context, Notification notification)
      throws IOException, DatasetManagementException {

      String programRunIdString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_RUN_ID);
      String programStatusString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_STATUS);

      ProgramStatus programStatus = null;
      try {
        programStatus = ProgramStatus.valueOf(programStatusString);
      } catch (IllegalArgumentException e) {
        LOG.warn("Invalid program status {} passed for programId {}", programStatusString, programRunIdString, e);
        // Fall through, let the thread return normally
      }

      // Ignore notifications which specify an invalid ProgramId, RunId, or ProgramStatus
      if (programRunIdString == null || programStatus == null) {
        return;
      }

      ProgramRunId programRunId = GSON.fromJson(programRunIdString, ProgramRunId.class);
      String triggerKeyForProgramStatus = Schedulers.triggerKeyForProgramStatus(programRunId.getParent(),
                                                                                programStatus);

      for (ProgramScheduleRecord schedule : getSchedules(context, triggerKeyForProgramStatus)) {
        if (schedule.getMeta().getStatus() == ProgramScheduleStatus.SCHEDULED) {
          // If the triggered program is a workflow, send the notification that contains the workflow token to be used
          if (schedule.getSchedule().getProgramId().getType() == ProgramType.WORKFLOW ||
              !notification.getProperties().containsKey(ProgramOptionConstants.WORKFLOW_TOKEN)) {
            getJobQueue().addNotification(schedule, notification);
          } else {
            // Otherwise send the notification without the workflow token
            Map<String, String> properties = new HashMap<>(notification.getProperties());
            properties.remove(ProgramOptionConstants.WORKFLOW_TOKEN);
            Notification programWithoutWorkflow = new Notification(Notification.Type.PROGRAM_STATUS, properties);
            getJobQueue().addNotification(schedule, programWithoutWorkflow);
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
