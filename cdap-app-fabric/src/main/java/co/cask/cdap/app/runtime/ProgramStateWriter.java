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

import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ProgramRunId;

import javax.annotation.Nullable;

/**
 * An interface that defines the behavior for how program states are persisted
 */
public interface ProgramStateWriter {

  /**
   * Updates the program run's status to be {@link ProgramRunStatus#STARTING} at the start time given by the
   * {@link ProgramRunId}
   *
   * @param programRunId the id of the program run
   * @param twillRunId the run id of the twill application
   */
  void start(ProgramRunId programRunId, ProgramOptions programOptions, @Nullable String twillRunId);

  /**
   * Updates the program run's status to be {@link ProgramRunStatus#RUNNING} at the given start time in seconds
   *
   * @param programRunId the id of the program run
   * @param twillRunId the run id of the twill application
   */
  void running(ProgramRunId programRunId, @Nullable String twillRunId);

  /**
   * Updates the program run's status to be completed
   *
   * @param programRunId the id of the program run
   * @param workflowToken the workflow token if the completed program was a workflow
   */
  void completed(ProgramRunId programRunId, @Nullable WorkflowToken workflowToken);

  /**
   * Updates the program run's status to be killed
   *
   * @param programRunId the id of the program run
   * @param workflowToken the workflow token if the killed program was a workflow
   */
  void killed(ProgramRunId programRunId, @Nullable WorkflowToken workflowToken);

  /**
   * Updates the program run's status to be failed with a specified failure cause
   *
   * @param programRunId the id of the program run
   * @param failureCause the cause of the failure
   * @param workflowToken the workflow token if the failed program was a workflow
   */
  void error(ProgramRunId programRunId, @Nullable WorkflowToken workflowToken, Throwable failureCause);

  /**
   * Updates the program run's status to be suspended
   *
   * @param programRunId the id of the program run
   */
  void suspend(ProgramRunId programRunId);

  /**
   * Updates the program run's status to be resumed
   *
   * @param programRunId the id of the program run
   */
  void resume(ProgramRunId programRunId);
}
