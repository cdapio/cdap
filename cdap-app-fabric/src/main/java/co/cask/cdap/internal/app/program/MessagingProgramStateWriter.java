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

package co.cask.cdap.internal.app.program;

import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * An implementation of ProgramStateWriter that publishes program status events to TMS
 */
public final class MessagingProgramStateWriter implements ProgramStateWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MessagingProgramStateWriter.class);
  private static final Gson GSON = new Gson();
  private final MessagingService messagingService;
  private final TopicId topicId;

  @Inject
  public MessagingProgramStateWriter(CConfiguration cConf, MessagingService messagingService) {
    this.topicId = NamespaceId.SYSTEM.topic(cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC));
    this.messagingService = messagingService;
  }

  @Override
  public void start(ProgramRunId programRunId, ProgramOptions programOptions, @Nullable String twillRunId) {
    long startTime = RunIds.getTime(RunIds.fromString(programRunId.getRun()), TimeUnit.MILLISECONDS);
    if (startTime == -1) {
      // If RunId is not time-based, use current time as start time
      startTime = System.currentTimeMillis();
    }
    publish(
      programRunId, twillRunId,
      ImmutableMap.<String, String>builder()
        .put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(startTime))
        .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramController.State.STARTING.getRunStatus().toString())
        .put(ProgramOptionConstants.USER_OVERRIDES, GSON.toJson(programOptions.getUserArguments().asMap()))
        .put(ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(programOptions.getArguments().asMap()))
    );
  }

  @Override
  public void running(ProgramRunId programRunId, @Nullable String twillRunId) {
    publish(
      programRunId, twillRunId,
      ImmutableMap.<String, String>builder()
        .put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(System.currentTimeMillis()))
        .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramController.State.ALIVE.getRunStatus().toString())
    );
  }

  @Override
  public void completed(ProgramRunId programRunId, @Nullable WorkflowToken workflowToken) {
    stop(programRunId, ProgramRunStatus.COMPLETED, workflowToken, null);
  }

  @Override
  public void killed(ProgramRunId programRunId, @Nullable WorkflowToken workflowToken) {
    stop(programRunId, ProgramRunStatus.KILLED, workflowToken, null);
  }

  @Override
  public void error(ProgramRunId programRunId, @Nullable WorkflowToken workflowToken, Throwable failureCause) {
    stop(programRunId, ProgramRunStatus.FAILED, workflowToken, failureCause);
  }

  @Override
  public void suspend(ProgramRunId programRunId) {
    publish(
      programRunId, null,
      ImmutableMap.<String, String>builder()
        .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.SUSPENDED.toString())
    );
  }

  @Override
  public void resume(ProgramRunId programRunId) {
    publish(
      programRunId, null,
      ImmutableMap.<String, String>builder()
        .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RESUMING.toString())
    );
  }

  private void stop(ProgramRunId programRunId, ProgramRunStatus runStatus, @Nullable WorkflowToken workflowToken,
                    Throwable cause) {
    ImmutableMap.Builder builder = ImmutableMap.<String, String>builder()
            .put(ProgramOptionConstants.END_TIME, String.valueOf(System.currentTimeMillis()))
            .put(ProgramOptionConstants.PROGRAM_STATUS, runStatus.toString());

    if (cause != null) {
      builder.put(ProgramOptionConstants.PROGRAM_ERROR, GSON.toJson(new BasicThrowable(cause)));
    }
    if (workflowToken != null) {
      builder.put(ProgramOptionConstants.WORKFLOW_TOKEN, GSON.toJson(workflowToken));
    }
    publish(programRunId, null, builder);
  }

  private void publish(ProgramRunId programRunId, String twillRunId, ImmutableMap.Builder<String, String> properties) {
    properties.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId));
    if (twillRunId != null) {
      properties.put(ProgramOptionConstants.TWILL_RUN_ID, twillRunId);
    }
    Notification programStatusNotification = new Notification(Notification.Type.PROGRAM_STATUS, properties.build());
    try {
      messagingService.publish(StoreRequestBuilder.of(topicId)
                                                  .addPayloads(GSON.toJson(programStatusNotification))
                                                  .build()
      );
    } catch (Exception e) {
      LOG.warn("Error while publishing notification for program {}: {}", programRunId, e);
    }
  }
}
