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
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
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
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * An implementation of ProgramStateWriter that publishes program status events to TMS
 */
public final class ProgramEventPublisher implements ProgramStateWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramEventPublisher.class);
  private static final Gson GSON = new Gson();
  private final Map<String, String> defaultProperties;
  private final MessagingService messagingService;
  private final TopicId topicId;
  private final ProgramId programId;
  private final RunId runId;
  private final String twillRunId;
  private final Arguments userArguments;
  private final Arguments systemArguments;
  private final WorkflowToken workflowToken;

  public ProgramEventPublisher(ProgramId programId, RunId runId, String twillRunId,
                               Arguments userArguments, Arguments systemArguments,
                               @Nullable WorkflowToken workflowToken,
                               CConfiguration cConf, MessagingService messagingService) {
    this.programId = programId;
    this.runId = runId;
    this.twillRunId = twillRunId;
    this.userArguments = userArguments;
    this.systemArguments = systemArguments;
    this.topicId = NamespaceId.SYSTEM.topic(cConf.get(Constants.Scheduler.PROGRAM_STATUS_EVENT_TOPIC));
    this.workflowToken = workflowToken;
    this.messagingService = messagingService;
    this.defaultProperties = ImmutableMap.of(
      ProgramOptionConstants.PROGRAM_ID, GSON.toJson(programId),
      ProgramOptionConstants.RUN_ID, runId.getId()
    );
  }

  @Override
  public void start(long startTimeInSeconds) {
    ImmutableMap.Builder builder = ImmutableMap.<String, String>builder()
      .putAll(defaultProperties)
      .put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(startTimeInSeconds))
      .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramController.State.STARTING.getRunStatus().toString())
      .put(ProgramOptionConstants.USER_OVERRIDES, GSON.toJson(userArguments.asMap()))
      .put(ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(systemArguments.asMap()));

    if (twillRunId != null) {
      builder.put(ProgramOptionConstants.TWILL_RUN_ID, twillRunId);
    }
    publish(builder.build());
  }

  @Override
  public void running(long startTimeInSeconds) {
    // Get start time from RunId
    startTimeInSeconds = RunIds.getTime(runId, TimeUnit.SECONDS);
    if (startTimeInSeconds == -1) {
      // If RunId is not time-based, use current time as start time
      startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    }
    ImmutableMap.Builder builder = ImmutableMap.<String, String>builder()
      .putAll(defaultProperties)
      .put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(startTimeInSeconds))
      .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramController.State.ALIVE.getRunStatus().toString())
      .put(ProgramOptionConstants.USER_OVERRIDES, GSON.toJson(userArguments.asMap()))
      .put(ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(systemArguments.asMap()));

    if (twillRunId != null) {
      builder.put(ProgramOptionConstants.TWILL_RUN_ID, twillRunId);
    }
    publish(builder.build());
  }

  @Override
  public void stop(long endTimeInSeconds, ProgramRunStatus runStatus, @Nullable BasicThrowable cause) {
    ImmutableMap.Builder builder = ImmutableMap.<String, String>builder()
      .putAll(defaultProperties)
      .put(ProgramOptionConstants.END_TIME, String.valueOf(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())))
      .put(ProgramOptionConstants.PROGRAM_STATUS, runStatus.toString());
    if (cause != null) {
      builder.put("error", GSON.toJson(cause));
    }
    if (workflowToken != null) {
      builder.put(ProgramOptionConstants.WORKFLOW_TOKEN, GSON.toJson(workflowToken));
    }
    publish(builder.build());
  }

  @Override
  public void suspend() {
    Map<String, String> properties =
      ImmutableMap.<String, String>builder().putAll(defaultProperties)
        .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.SUSPENDED.toString())
        .build();
    publish(properties);
  }

  @Override
  public void resume() {
    Map<String, String> properties =
      ImmutableMap.<String, String>builder().putAll(defaultProperties)
        .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RUNNING.toString())
        .build();
    publish(properties);
  }

  private void publish(Map<String, String> properties) {
    Notification programStatusNotification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    try {
      messagingService.publish(StoreRequestBuilder.of(topicId)
                                                  .addPayloads(GSON.toJson(programStatusNotification))
                                                  .build()
      );
    } catch (Exception e) {
      LOG.warn("Error while publishing notification for program {}: {}", programId.getProgram(), e);
    }
  }

  public ProgramId getProgramId() {
    return programId;
  }

  public RunId getRunId() {
    return runId;
  }
}
