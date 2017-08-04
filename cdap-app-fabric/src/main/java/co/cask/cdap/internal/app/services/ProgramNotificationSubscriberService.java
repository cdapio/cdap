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

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Service that receives program status notifications and persists to the store
 */
public class ProgramNotificationSubscriberService extends AbstractNotificationSubscriberService {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramNotificationSubscriberService.class);
  private static final Gson GSON = new Gson();
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();

  private final CConfiguration cConf;
  private ExecutorService taskExecutorService;
  private final DatasetFramework datasetFramework;
  private final MessagingService messagingService;

  @Inject
  ProgramNotificationSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                       DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    super(messagingService, cConf, datasetFramework, txClient);
    this.cConf = cConf;
    this.datasetFramework = datasetFramework;
    this.messagingService = messagingService;
  }

  @Override
  protected void startUp() {
    LOG.info("Starting ProgramNotificationSubscriberService");
    taskExecutorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                                                          .setNameFormat("program-status-subscriber-task-%d")
                                                          .build());
    taskExecutorService.submit(new ProgramStatusNotificationSubscriberThread(
      cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC)));
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
    LOG.info("Stopped ProgramNotificationSubscriberService.");
  }

  /**
   * Thread that receives TMS notifications and persists the program status notification to the store
   */
  private class ProgramStatusNotificationSubscriberThread extends NotificationSubscriberThread {
    private final String topic;

    ProgramStatusNotificationSubscriberThread(String topic) {
      super(topic);
      this.topic = topic;
    }

    @Override
    public String loadMessageId(DatasetContext context) {
      return getAppMetadataStore(context).retrieveSubscriberState(topic);
    }

    @Override
    public void updateMessageId(DatasetContext context, String lastFetchedMessageId) {
      getAppMetadataStore(context).persistSubscriberState(topic, lastFetchedMessageId);
    }

    @Override
    public void processNotification(final DatasetContext context, Notification notification)
      throws IOException, DatasetManagementException {

      Map<String, String> properties = notification.getProperties();
      // Required parameters
      String programRunIdString = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
      String programStatusString = properties.get(ProgramOptionConstants.PROGRAM_STATUS);

      // Ignore notifications which specify an invalid ProgramRunId or ProgramRunStatus
      if (programRunIdString == null || programStatusString == null) {
        return;
      }

      ProgramRunStatus programRunStatus = null;
      if (programStatusString != null) {
        try {
          programRunStatus = ProgramRunStatus.valueOf(programStatusString);
        } catch (IllegalArgumentException e) {
          LOG.warn("Invalid program run status {} passed in notification for program {}",
                   programStatusString, programRunIdString);
          return;
        }
      }

      ProgramRunId programRunId = GSON.fromJson(programRunIdString, ProgramRunId.class);
      ProgramId programId = programRunId.getParent();
      String runId = programRunId.getRun();
      String twillRunId = notification.getProperties().get(ProgramOptionConstants.TWILL_RUN_ID);
      long endTimeSecs = getTimeSeconds(notification.getProperties(), ProgramOptionConstants.END_TIME);

      switch(programRunStatus) {
        case STARTING:
          long startTimeSecs = getTimeSeconds(notification.getProperties(), ProgramOptionConstants.START_TIME);
          String userArgumentsString = properties.get(ProgramOptionConstants.USER_OVERRIDES);
          String systemArgumentsString = properties.get(ProgramOptionConstants.SYSTEM_OVERRIDES);
          if (userArgumentsString == null || systemArgumentsString == null) {
            LOG.warn((userArgumentsString == null) ? "user" : "system" + " arguments was not specified in program " +
                     "status notification for program run " + programRunId);
            return;
          }
          if (startTimeSecs == -1) {
            LOG.warn("Start time was not specified in program notification for program run " + programRunId);
            return;
          }
          Map<String, String> userArguments = GSON.fromJson(userArgumentsString, STRING_STRING_MAP);
          Map<String, String> systemArguments = GSON.fromJson(systemArgumentsString, STRING_STRING_MAP);
          getAppMetadataStore(context).recordProgramStart(programId, runId, startTimeSecs, twillRunId,
                                                          userArguments, systemArguments);
          break;
        case RUNNING:
          long logicalStartTimeSecs = getTimeSeconds(notification.getProperties(),
                                                     ProgramOptionConstants.LOGICAL_START_TIME);
          if (logicalStartTimeSecs == -1) {
            LOG.warn("Logical start time was not specified in program notification for program run " + programRunId);
            return;
          }

          getAppMetadataStore(context).recordProgramRunning(programId, runId, logicalStartTimeSecs, twillRunId);
          break;
        case SUSPENDED:
          getAppMetadataStore(context).recordProgramSuspend(programId, runId);
          break;
        case RESUMING:
          getAppMetadataStore(context).recordProgramResumed(programId, runId);
          break;
        case COMPLETED:
        case KILLED:
          if (endTimeSecs == -1) {
            LOG.warn("Run time was not specified in program running notification for program run " + programRunId);
            return;
          }
          getAppMetadataStore(context).recordProgramStop(programId, runId, endTimeSecs, programRunStatus, null);
          break;
        case FAILED:
          if (endTimeSecs == -1) {
            LOG.warn("Run time was not specified in program running notification for program run " + programRunId);
            return;
          }
          String errorString = properties.get(ProgramOptionConstants.PROGRAM_ERROR);
          BasicThrowable cause = (errorString == null) ? null : GSON.fromJson(errorString, BasicThrowable.class);
          getAppMetadataStore(context).recordProgramStop(programId, runId, endTimeSecs, programRunStatus, cause);
          break;
        default:
          LOG.error("Cannot persist ProgramRunStatus %s for Program %s", programRunStatus, programRunId);
          return;
      }

      ProgramStatus programStatus;
      try {
        programStatus = ProgramRunStatus.toProgramStatus(programRunStatus);
      } catch (IllegalArgumentException e) {
        // Return silently, this happens for statuses that are not meant to be scheduled
        return;
      }

      // Forward the notification to the scheduler only if it can trigger at least one other program
      if (canTriggerOtherPrograms(context, Schedulers.triggerKeyForProgramStatus(programRunId.getParent(),
                                                                                 programStatus))) {
        TopicId programStatusTriggerTopic =
          NamespaceId.SYSTEM.topic(cConf.get(Constants.Scheduler.PROGRAM_STATUS_EVENT_TOPIC));
        try {
          messagingService.publish(StoreRequestBuilder.of(programStatusTriggerTopic)
                                     .addPayloads(GSON.toJson(notification))
                                     .build());
        } catch (Exception e) {
          // Will be retried when re-processing
          LOG.error("Failed to publish messages to TMS: ", e);
          Throwables.propagate(e);
        }
      }
    }

    /**
     * Helper method to extract the time from the given properties map, or return -1 if no value was found
     *
     * @param properties the properties map
     * @param option the key to lookup in the properties map
     * @return the time in seconds, or -1 if not found
     */
    private long getTimeSeconds(Map<String, String> properties, String option) {
      String timeString = properties.get(option);
      return (timeString == null) ? -1 : TimeUnit.MILLISECONDS.toSeconds(Long.valueOf(timeString));
    }

    private boolean canTriggerOtherPrograms(DatasetContext context, String triggerKey)
      throws IOException, DatasetManagementException {
      return !Schedulers.getScheduleStore(context, datasetFramework).findSchedules(triggerKey).isEmpty();
    }
  }
}
