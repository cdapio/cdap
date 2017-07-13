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
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
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
  private final RuntimeStore store;
  private final ExecutorService taskExecutorService;
  private final DatasetFramework datasetFramework;
  private final MessagingService messagingService;

  @Inject
  ProgramNotificationSubscriberService(MessagingService messagingService, RuntimeStore store, CConfiguration cConf,
                                       DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    super(messagingService, cConf, datasetFramework, txClient);
    this.cConf = cConf;
    this.store = store;
    this.taskExecutorService =
      Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("program-status-subscriber-task-%d")
                                                              .build());
    this.messagingService = messagingService;
    this.datasetFramework = datasetFramework;
  }

  @Override
  protected void startUp() {
    LOG.info("Starting ProgramNotificationSubscriberService");

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

    ProgramStatusNotificationSubscriberThread(String topic) {
      super(topic);
    }

    @Override
    public String loadMessageId() {
      return null;
    }

    @Override
    public void processNotification(DatasetContext context, Notification notification) throws Exception {
      Map<String, String> properties = notification.getProperties();
      // Required parameters
      String programRunIdString = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
      String programStatusString = properties.get(ProgramOptionConstants.PROGRAM_STATUS);

      ProgramRunStatus runStatus = null;
      if (programStatusString != null) {
        try {
          runStatus = ProgramRunStatus.valueOf(programStatusString);
        } catch (IllegalArgumentException e) {
          LOG.warn("Invalid program run status {} passed in notification for program {}",
                   programStatusString, programRunIdString);
        }
      }

      // Ignore notifications which specify an invalid ProgramRunId or ProgramRunStatus
      if (programRunIdString == null || runStatus == null) {
        return;
      }

      final ProgramRunId programRunId = GSON.fromJson(programRunIdString, ProgramRunId.class);
      final String twillRunId = notification.getProperties().get(ProgramOptionConstants.TWILL_RUN_ID);
      final Map<String, String> userArguments = getArguments(properties, ProgramOptionConstants.USER_OVERRIDES);
      final Map<String, String> systemArguments = getArguments(properties, ProgramOptionConstants.SYSTEM_OVERRIDES);

      final long stateChangeTime = getTime(notification.getProperties(), ProgramOptionConstants.LOGICAL_START_TIME);
      final long endTime = getTime(notification.getProperties(), ProgramOptionConstants.END_TIME);
      final ProgramRunStatus programRunStatus = runStatus;
//      System.out.println("PERSIST PROGRAM " + programRunId + " STATUS " + programRunStatus);
      switch(programRunStatus) {
        case STARTING:
          if (stateChangeTime == -1) {
            throw new IllegalArgumentException("Start time was not specified in program starting notification for " +
                                               "program run {}" + programRunId);
          }
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              store.setStart(programRunId.getParent(), programRunId.getRun(),
                             TimeUnit.MILLISECONDS.toSeconds(stateChangeTime), twillRunId,
                             userArguments, systemArguments);
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
          break;
        case RUNNING:
          if (stateChangeTime == -1) {
            throw new IllegalArgumentException("Run time was not specified in program running notification for " +
                                               "program run {}" + programRunId);
          }
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              store.setRunning(programRunId.getParent(), programRunId.getRun(),
                               TimeUnit.MILLISECONDS.toSeconds(stateChangeTime), twillRunId);
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
          break;
        case SUSPENDED:
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              store.setSuspend(programRunId.getParent(), programRunId.getRun());
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
          break;
        case RESUMING:
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              store.setResume(programRunId.getParent(), programRunId.getRun());
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
          break;
        case COMPLETED:
        case KILLED:
          if (endTime == -1) {
            throw new IllegalArgumentException("End time was not specified in program status notification for " +
                                               "program run {}" + programRunId);
          }
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              store.setStop(programRunId.getParent(), programRunId.getRun(), TimeUnit.MILLISECONDS.toSeconds(endTime),
                            programRunStatus, null);
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
          break;
        case FAILED:
          if (endTime == -1) {
            throw new IllegalArgumentException("End time was not specified in program status notification for " +
                                               "program run {}" + programRunId);
          }
          String errorString = properties.get(ProgramOptionConstants.PROGRAM_ERROR);
          final BasicThrowable cause = (errorString == null)
            ? null
            : GSON.fromJson(errorString, BasicThrowable.class);
          Retries.supplyWithRetries(new Supplier<Void>() {
            @Override
            public Void get() {
              store.setStop(programRunId.getParent(), programRunId.getRun(), TimeUnit.MILLISECONDS.toSeconds(endTime),
                            programRunStatus, cause);
              return null;
            }
          }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
          break;
        default:
          throw new UnsupportedOperationException(String.format("Cannot persist ProgramRunStatus %s for Program %s",
                                                                programRunStatus, programRunId));
      }

      // TODO hack to prevent converting ProgramRunStatus to ProgramStatus for STARTING - is there a better way?
      // Can we add ProgramStatus#STARTING? Since ProgramRunStatus should be deprecated anyways
      // Do we map STARTING to INITIALIZING?
      if (programRunStatus != ProgramRunStatus.STARTING) {
        ProgramStatus programStatus = ProgramStatus.valueOf(programRunStatus.toString().toUpperCase());
        String triggerKeyForProgramStatus = Schedulers.triggerKeyForProgramStatus(programRunId.getParent(),
                                                                                  programStatus);

        if (canTriggerOtherPrograms(context, triggerKeyForProgramStatus)) {
          // Now forward the notification to the scheduler
          TopicId programStatusTriggerTopic =
            NamespaceId.SYSTEM.topic(cConf.get(Constants.Scheduler.PROGRAM_STATUS_EVENT_TOPIC));
          messagingService.publish(StoreRequestBuilder.of(programStatusTriggerTopic)
                                                      .addPayloads(GSON.toJson(notification))
                                                      .build());
        }
      }
    }

    private long getTime(Map<String, String> properties, String option) {
      String timeString = properties.get(option);
      return (timeString == null) ? -1 : Long.valueOf(timeString);
    }

    private Map<String, String> getArguments(Map<String, String> properties, String option) {
      String argumentsString = properties.get(option);
      if (argumentsString == null) {
        return ImmutableMap.of();
      }
      Map<String, String> arguments = GSON.fromJson(argumentsString, STRING_STRING_MAP);
      return (arguments == null) ? new BasicArguments().asMap()
                                 : new BasicArguments(arguments).asMap();
    }
  }

  private boolean canTriggerOtherPrograms(DatasetContext context, String triggerKey)
          throws IOException, DatasetManagementException {
    return !Schedulers.getScheduleStore(context, datasetFramework).findSchedules(triggerKey).isEmpty();
  }
}
