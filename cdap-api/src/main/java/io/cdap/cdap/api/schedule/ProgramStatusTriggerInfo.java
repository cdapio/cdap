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

package io.cdap.cdap.api.schedule;

import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.workflow.WorkflowToken;
import org.apache.twill.api.RunId;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * The program status trigger information to be passed to the triggered program.
 */
public interface ProgramStatusTriggerInfo extends TriggerInfo {

  /**
   * @return The namespace of the triggering program.
   */
  String getNamespace();

  /**
   * @return The name of the application that contains the triggering program.
   */
  String getApplicationName();

  /**
   * @return The program type of the triggering program.
   */
  ProgramType getProgramType();

  /**
   * @return The program name of the triggering program.
   */
  String getProgram();

  /**
   * @return The program run Id of the triggering program run that can satisfy the program status trigger.
   */
  RunId getRunId();

  /**
   * @return The program status of the triggering program run that can satisfy the program status trigger.
   */
  ProgramStatus getProgramStatus();

  /**
   * @return The workflow token if the program is a workflow, or {@code null} otherwise.
   */
  @Nullable
  WorkflowToken getWorkflowToken();

  /**
   * @return An immutable map of the runtime arguments of the triggering program run that can
   *         satisfy the program status trigger.
   */
  Map<String, String> getRuntimeArguments();
}
