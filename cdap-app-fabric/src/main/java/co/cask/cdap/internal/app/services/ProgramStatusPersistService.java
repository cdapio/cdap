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

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.store.ProgramStorePublisher;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Service that receives program statuses and persists to the store
 */
public class ProgramStatusPersistService extends AbstractNotificationSubscriberService {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramStatusPersistService.class);
  private static final Gson GSON = new Gson();
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();

  @Inject
  ProgramStatusPersistService(MessagingService messagingService, Store store, CConfiguration cConf,
                              DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    super(messagingService, store, cConf, datasetFramework, txClient);
  }

  @Override
  protected void startUp() {
    LOG.info("Starting ProgramStatusPersistService");

    taskExecutorService =
      Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("program-status-subscriber-task-%d")
                                                              .build());
    taskExecutorService.submit(new ProgramStatusSubscriberThread(
      cConf.get(Constants.Scheduler.PROGRAM_STATUS_EVENT_TOPIC)));
  }

  private class ProgramStatusSubscriberThread
      extends AbstractNotificationSubscriberService.NotificationSubscriberThread {

    ProgramStatusSubscriberThread(String topic) {
      super(topic);
    }

    @Override
    protected String loadMessageId() {
      return null;
    }

    @Override
    protected void processNotification(DatasetContext context, Notification notification) throws Exception {
      // Required parameters
      Map<String, String> properties = notification.getProperties();
      String programIdString = properties.get(ProgramOptionConstants.PROGRAM_ID);
      String runIdString = properties.get(ProgramOptionConstants.RUN_ID);
      String programStatusString = properties.get(ProgramOptionConstants.PROGRAM_STATUS);

      ProgramRunStatus programRunStatus = null;
      if (programStatusString != null) {
        try {
          programRunStatus = ProgramRunStatus.valueOf(programStatusString);
        } catch (IllegalArgumentException e) {
          LOG.warn("Invalid program status {} passed for program {}", programStatusString, programIdString, e);
          // Fall through, let the thread return normally
        }
      }

      // Ignore notifications which specify an invalid ProgramId, RunId, or ProgramRunStatus
      if (programIdString == null || runIdString == null || programRunStatus == null) {
        return;
      }

      ProgramId programId = GSON.fromJson(programIdString, ProgramId.class);
      String twillRunId = notification.getProperties().get(ProgramOptionConstants.TWILL_RUN_ID);
      Arguments userArguments = getArguments(properties, ProgramOptionConstants.USER_OVERRIDES);
      Arguments systemArguments = getArguments(properties, ProgramOptionConstants.SYSTEM_OVERRIDES);
      ProgramStateWriter programStateWriter =
        new ProgramStorePublisher(programId, RunIds.fromString(runIdString), twillRunId,
                                  userArguments, systemArguments, store);

      long startTime = getTime(notification.getProperties(), ProgramOptionConstants.LOGICAL_START_TIME);
      long endTime = getTime(notification.getProperties(), ProgramOptionConstants.END_TIME);
      System.out.println("PERSIST " + programId + " with Status " + programRunStatus);
      switch(programRunStatus) {
        case STARTING:
          if (startTime == -1) {
            LOG.debug("Start time not specified in notification for program id {}, not persisting" + programId);
            return;
          }
          programStateWriter.start(startTime);
          break;
        case RUNNING:
          if (startTime == -1) {
            LOG.debug("Start time not specified in notification for program id {}, not persisting" + programId);
            return;
          }
          programStateWriter.running(startTime);
          break;
        case COMPLETED:
        case SUSPENDED:
        case KILLED:
          if (endTime == -1) {
            LOG.debug("End time not specified in notification for program id {}, not persisting" + programId);
            return;
          }
          programStateWriter.stop(endTime, programRunStatus, null);
          break;
        case FAILED:
          if (endTime == -1) {
            LOG.debug("End time not specified in notification for program id {}, not persisting" + programId);
            return;
          }
          String throwableString = properties.get("error");
          BasicThrowable cause = GSON.fromJson(throwableString, BasicThrowable.class);
          programStateWriter.stop(endTime, ProgramRunStatus.FAILED, cause);
          break;
        default:
          throw new IllegalArgumentException(String.format("Cannot persist ProgramRunStatus %s for ProgramId %s",
                                                           programRunStatus.toString(), programId));
      }
    }

    private long getTime(Map<String, String> properties, String option) {
      String timeString = properties.get(option);
      return (timeString == null) ? -1 : Long.valueOf(timeString);
    }

    private Arguments getArguments(Map<String, String> properties, String option) {
      String argumentsString = properties.get(option);
      Map<String, String> arguments = GSON.fromJson(argumentsString, STRING_STRING_MAP);
      return (arguments == null) ? new BasicArguments()
                                 : new BasicArguments(arguments);
    }
  }
}
