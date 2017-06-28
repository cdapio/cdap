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
import com.google.gson.Gson;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Event publisher that publishes system-level program state events
 */
public class ProgramEventPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramEventPublisher.class);
  private static final Gson GSON = new Gson();
  private final CConfiguration cConf;
  private final MessagingService messagingService;
  private final TopicId topicId;
  private long startTimeInSeconds;

  @Inject
  public ProgramEventPublisher(CConfiguration cConf, MessagingService messagingService) {
    this.cConf = cConf;
    this.messagingService = messagingService;
    this.topicId = NamespaceId.SYSTEM.topic(this.cConf.get(Constants.Scheduler.PROGRAM_STATUS_EVENT_TOPIC));
  }

  public void recordProgramStart(long startTimeInSeconds) {
    this.startTimeInSeconds = startTimeInSeconds;
  }

  /**
   * Sends a notification under the program status event topic about the status of a program
   *
   * @param programId the program id
   * @param runId the program run id
   * @param programRunStatus the run status of the program
   * @param userArguments the user arguments of the program
   * @param token the workflow token to contain payload information from a completed workflow
   */
  public void publishNotification(ProgramId programId, RunId runId, ProgramRunStatus programRunStatus,
                                  Arguments userArguments, @Nullable WorkflowToken token) {
    Map<String, String> properties = new HashMap<>();
    properties.put(ProgramOptionConstants.RUN_ID, runId.getId());
    properties.put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(startTimeInSeconds));
    properties.put(ProgramOptionConstants.PROGRAM_ID, programId.toString());
    properties.put(ProgramOptionConstants.PROGRAM_STATUS, programRunStatus.toProgramStatus().toString());
    properties.put(ProgramOptionConstants.USER_OVERRIDES, GSON.toJson(userArguments.asMap()));
    if (token != null) { // WorkflowToken is null if the triggering program is not a workflow
      properties.put(ProgramOptionConstants.WORKFLOW_TOKEN, GSON.toJson(token));
    }

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
