/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.workflow.ProgramWorkflowAction;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Factory for {@link ProgramWorkflowRunner} which returns the appropriate {@link ProgramWorkflowRunner}
 * depending upon the program from the {@link SchedulableProgramType}.
 * It acts as the single point for conditionally creating the needed {@link ProgramWorkflowRunner} for programs.
 * Currently we support {@link MapReduce} and {@link Spark} in Workflow (See {@link SchedulableProgramType}.
 */
public class ProgramWorkflowRunnerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowDriver.class);

  private final WorkflowSpecification workflowSpec;
  private final ProgramRunnerFactory programRunnerFactory;
  private final Program workflowProgram;
  private final RunId runId;
  private final Arguments userArguments;
  private final long logicalStartTime;

  public ProgramWorkflowRunnerFactory(WorkflowSpecification workflowSpec, ProgramRunnerFactory programRunnerFactory,
                                      Program workflowProgram, RunId runId, Arguments runtimeArguments,
                                      long logicalStartTime) {
    this.workflowSpec = workflowSpec;
    this.programRunnerFactory = programRunnerFactory;
    this.workflowProgram = workflowProgram;
    this.runId = runId;
    this.userArguments = runtimeArguments;
    this.logicalStartTime = logicalStartTime;
  }

  /**
   * Gives the appropriate instance of {@link ProgramWorkflowRunner} depending upon the program type found in the
   * properties of the {@link WorkflowActionSpecification}.
   *
   * @param actionSpec The {@link WorkflowActionSpecification}
   * @return the appropriate concrete implementation of {@link ProgramWorkflowRunner} for the program
   */
  public ProgramWorkflowRunner getProgramWorkflowRunner(WorkflowActionSpecification actionSpec) {


    if (actionSpec.getProperties().containsKey(ProgramWorkflowAction.PROGRAM_TYPE)) {
      switch (SchedulableProgramType.valueOf(actionSpec.getProperties().get(ProgramWorkflowAction.PROGRAM_TYPE))) {
        case MAPREDUCE:
          return new MapReduceProgramWorkflowRunner(workflowSpec, programRunnerFactory, workflowProgram, runId,
                                                    userArguments, logicalStartTime);
        case SPARK:
          return new SparkProgramWorkflowRunner(workflowSpec, programRunnerFactory, workflowProgram, runId,
                                                userArguments, logicalStartTime);
        default:
          LOG.debug("No workflow program runner found for this program");
      }
    } else {
      LOG.debug("ProgramType key not found in Workflow Action Specification Properties");
    }
    return null;  // if no workflow program runner was found for this program
  }
}
