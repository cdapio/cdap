/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.TopicId;
import com.google.gson.Gson;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides method to create {@link PluginInstantiator} for Program Runners
 */
public abstract class AbstractProgramRunnerWithPlugin implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramRunnerWithPlugin.class);
  protected final CConfiguration cConf;
  protected final MessagingService messagingService;
  private static final Gson GSON = new Gson();

  public AbstractProgramRunnerWithPlugin(CConfiguration cConf, MessagingService messagingService) {
    this.cConf = cConf;
    this.messagingService = messagingService;
  }

  /**
   * Creates a {@link PluginInstantiator} based on the {@link ProgramOptionConstants#PLUGIN_DIR} in
   * the system arguments in the given {@link ProgramOptions}.
   *
   * @param options the program runner options
   * @param classLoader the parent ClassLoader for the {@link PluginInstantiator} to use
   * @return A new {@link PluginInstantiator} or {@code null} if no plugin is available.
   */
  @Nullable
  protected PluginInstantiator createPluginInstantiator(ProgramOptions options, ClassLoader classLoader) {
    if (!options.getArguments().hasOption(ProgramOptionConstants.PLUGIN_DIR)) {
      return null;
    }
    return new PluginInstantiator(
      cConf, classLoader, new File(options.getArguments().getOption(ProgramOptionConstants.PLUGIN_DIR)));
  }

  /**
   * Sends a notification under the program status event topic about the status of a program
   *
   * @param programId the program id
   * @param runId the program run id
   * @param programStatus the program status
   * @param userArguments the user arguments of the program
   * @param token the workflow token to contain payload information from a completed workflow
   */
  protected void sendProgramStatusNotification(ProgramId programId, RunId runId, ProgramStatus programStatus,
                                               Arguments userArguments, @Nullable WorkflowToken token) {
    // Since we don't know which other schedules depends on this program, we can't use ScheduleTaskPublisher
    Map<String, String> properties = new HashMap<String, String>();
    properties.put(ProgramOptionConstants.RUN_ID, runId.getId());
    properties.put(ProgramOptionConstants.PROGRAM_ID, programId.toString());
    properties.put(ProgramOptionConstants.PROGRAM_STATUS, programStatus.toString());
    properties.put(ProgramOptionConstants.USER_OVERRIDES, GSON.toJson(userArguments.asMap()));
    if (token != null) {
      properties.put(ProgramOptionConstants.WORKFLOW_TOKEN, GSON.toJson(token));
    }

    Notification programStatusNotification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    TopicId topicId = NamespaceId.SYSTEM.topic(cConf.get(Constants.Scheduler.PROGRAM_STATUS_EVENT_TOPIC));
    try {
      messagingService.publish(StoreRequestBuilder.of(topicId)
                      .addPayloads(GSON.toJson(programStatusNotification))
                      .build()
      );
    } catch (TopicNotFoundException | IOException e) {
      LOG.warn("Error while publishing notification for program {}: {}", programId.getProgram(), e);
      // TODO throw new exception
    }
  }
}
