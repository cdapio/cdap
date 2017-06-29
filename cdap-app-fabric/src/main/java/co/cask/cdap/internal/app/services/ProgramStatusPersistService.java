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
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
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
import org.apache.twill.api.RunId;
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
  ProgramStatusPersistService(MessagingService messagingService, RuntimeStore runtimeStore, CConfiguration cConf,
                              DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    super(messagingService, runtimeStore, cConf, datasetFramework, txClient);
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
      String programIdString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_ID);
      String runIdString = notification.getProperties().get(ProgramOptionConstants.RUN_ID);
      String twillRunIdString = notification.getProperties().get(ProgramOptionConstants.TWILL_RUN_ID);

      String startTimeString = notification.getProperties().get(ProgramOptionConstants.LOGICAL_START_TIME);
      String endTimeString = notification.getProperties().get(ProgramOptionConstants.END_TIME);
      long startTime = (startTimeString == null) ? -1 : Long.valueOf(startTimeString);
      long endTime = (endTimeString == null) ? -1 : Long.valueOf(endTimeString);

      String userOverridesString = notification.getProperties().get(ProgramOptionConstants.USER_OVERRIDES);
      String systemOverridesString = notification.getProperties().get(ProgramOptionConstants.SYSTEM_OVERRIDES);
      String programStatusString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_STATUS);
      String basicThrowableString = notification.getProperties().get("error");
      ProgramRunStatus programRunStatus = null;
      if (programStatusString != null) {
        try {
          programRunStatus = ProgramRunStatus.valueOf(programStatusString);
        } catch (IllegalArgumentException e) {
          LOG.warn("Invalid program status {} passed for programId {}", programStatusString, programIdString, e);
          // Fall through, let the thread return normally
        }
      }

      // Ignore notifications which specify an invalid ProgramId, RunId, or ProgramRunStatus
      if (programIdString == null || runIdString == null || programRunStatus == null) {
        return;
      }
      ProgramId programId = GSON.fromJson(programIdString, ProgramId.class);
      String twillRunId = GSON.fromJson(twillRunIdString, RunId.class).getId();
      Map<String, String> userOverrides = GSON.fromJson(userOverridesString, STRING_STRING_MAP);
      Map<String, String> systemOverrides = GSON.fromJson(systemOverridesString, STRING_STRING_MAP);

      switch(programRunStatus) {
        case STARTING:
          if (startTime == -1) {
            LOG.debug("Start time not specified in notification for program id {}, not persisting" + programId);
            return;
          }
          runtimeStore.setInit(programId, runIdString, startTime, twillRunId, userOverrides, systemOverrides);
          break;
        case RUNNING:
          if (startTime == -1) {
            LOG.debug("Start time not specified in notification for program id {}, not persisting" + programId);
            return;
          }
          runtimeStore.setStart(programId, runIdString, startTime, twillRunId, userOverrides, systemOverrides);
          break;
        case COMPLETED:
        case SUSPENDED:
        case KILLED:
          if (endTime == -1) {
            LOG.debug("End time not specified in notification for program id {}, not persisting" + programId);
            return;
          }
          runtimeStore.setStop(programId, runIdString, endTime, programRunStatus);
          break;
        case FAILED:
          if (endTime == -1) {
            LOG.debug("End time not specified in notification for program id {}, not persisting" + programId);
            return;
          }
          BasicThrowable cause = GSON.fromJson(basicThrowableString, BasicThrowable.class);
          runtimeStore.setStop(programId, runIdString, endTime, ProgramRunStatus.FAILED, cause);
          break;
        default:
          throw new IllegalArgumentException(String.format("Cannot persist ProgramRunStatus %s for ProgramId %s",
                                                           programRunStatus.toString(), programId));
      }
    }
  }
}
