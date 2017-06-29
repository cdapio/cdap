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

package co.cask.cdap.app.runtime;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
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
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Event publisher that publishes system-level program state events
 */
public class ProgramEventPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramEventPublisher.class);
  private static final Gson GSON = new Gson();
  private Map<String, String> defaultProperties;
  private final CConfiguration cConf;
  private final MessagingService messagingService;
  private final TopicId topicId;
  private final ProgramId programId;
  private final RunId runId;
  private final AtomicReference<ProgramController.State> state;

  public ProgramEventPublisher(CConfiguration cConf, MessagingService messagingService, ProgramId programId,
                               RunId runId) {
    this.cConf = cConf;
    this.messagingService = messagingService;
    this.programId = programId;
    this.runId = runId;
    this.defaultProperties = ImmutableMap.of(
      ProgramOptionConstants.PROGRAM_ID, programId.toString(),
      ProgramOptionConstants.RUN_ID, GSON.toJson(runId.getId())
    );
    this.topicId = NamespaceId.SYSTEM.topic(this.cConf.get(Constants.Scheduler.PROGRAM_STATUS_EVENT_TOPIC));
    this.state = new AtomicReference<>(ProgramController.State.STARTING);
  }

  public void running(String twillRunId, long startTimeInSeconds, Arguments userArgs, Arguments systemArgs) {
    if (!this.state.compareAndSet(ProgramController.State.STARTING, ProgramController.State.ALIVE)) {
      LOG.warn("Invalid transition from {} to State.ALIVE", this.state);
      return;
    }

    Map<String, String> properties =
      ImmutableMap.<String, String>builder().putAll(defaultProperties)
        .put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(startTimeInSeconds))
        .put(ProgramOptionConstants.PROGRAM_ID, programId.toString())
        .put(ProgramOptionConstants.USER_OVERRIDES, GSON.toJson(userArgs.asMap()))
        .put(ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(systemArgs.asMap()))
        .build();
    publish(properties);
  }

  public void stop(long endTimeInSeconds, ProgramRunStatus runStatus, @Nullable Throwable cause) {
    ImmutableMap.Builder builder = ImmutableMap.<String, String>builder()
      .putAll(defaultProperties)
      .put(ProgramOptionConstants.END_TIME, String.valueOf(endTimeInSeconds))
      .put(ProgramOptionConstants.PROGRAM_STATUS, runStatus.toString());
    if (cause != null) {
      builder.put("error", GSON.toJson(cause));
    }
    publish(builder.build());
  }


  public void stop(long endTimeInSeconds, ProgramRunStatus runStatus, WorkflowToken workflowToken,
                   @Nullable Throwable cause) {
    Map<String, String> properties =
      ImmutableMap.<String, String>builder().putAll(defaultProperties)
        .put(ProgramOptionConstants.END_TIME, String.valueOf(endTimeInSeconds))
        .put(ProgramOptionConstants.PROGRAM_STATUS, runStatus.toString())
        .put(ProgramOptionConstants.WORKFLOW_TOKEN, GSON.toJson(workflowToken))
        .build();
    publish(properties);
  }

  public void suspend() {
    Map<String, String> properties =
      ImmutableMap.<String, String>builder().putAll(defaultProperties)
        .put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.SUSPENDED.toString())
        .build();
    publish(properties);
  }

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
}
