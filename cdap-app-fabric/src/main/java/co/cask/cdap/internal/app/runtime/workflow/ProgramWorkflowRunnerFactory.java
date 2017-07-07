/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.api.workflow.WorkflowNodeState;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.proto.ProgramType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A Factory for {@link ProgramWorkflowRunner} which returns the appropriate {@link ProgramWorkflowRunner}
 * depending upon the program from the {@link SchedulableProgramType}.
 * It acts as the single point for conditionally creating the needed {@link ProgramWorkflowRunner} for programs.
 * Currently we support {@link MapReduce} and {@link Spark} in Workflow (See {@link SchedulableProgramType}.
 */
final class ProgramWorkflowRunnerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramWorkflowRunnerFactory.class);

  private final CConfiguration cConf;
  private final WorkflowSpecification workflowSpec;
  private final ProgramRunnerFactory programRunnerFactory;
  private final Program workflowProgram;
  private final ProgramOptions workflowProgramOptions;

  ProgramWorkflowRunnerFactory(CConfiguration cConf, WorkflowSpecification workflowSpec,
                               ProgramRunnerFactory programRunnerFactory,
                               Program workflowProgram, ProgramOptions workflowProgramOptions) {
    this.cConf = cConf;
    this.workflowSpec = workflowSpec;
    this.programRunnerFactory = programRunnerFactory;
    this.workflowProgram = workflowProgram;
    this.workflowProgramOptions = workflowProgramOptions;
  }

  /**
   * Gives the appropriate instance of {@link ProgramWorkflowRunner} depending upon the program type found in the
   * properties of the {@link WorkflowActionSpecification}.
   *
   * @param actionSpec the {@link WorkflowActionSpecification}
   * @param token the {@link WorkflowToken}
   * @param nodeStates the map of node ids to node states
   * @return the appropriate concrete implementation of {@link ProgramWorkflowRunner} for the program
   */
  ProgramWorkflowRunner getProgramWorkflowRunner(WorkflowActionSpecification actionSpec, WorkflowToken token,
                                                 String nodeId, Map<String, WorkflowNodeState> nodeStates) {

    if (actionSpec.getProperties().containsKey(ProgramWorkflowAction.PROGRAM_TYPE)) {
      switch (SchedulableProgramType.valueOf(actionSpec.getProperties().get(ProgramWorkflowAction.PROGRAM_TYPE))) {
        case MAPREDUCE:
          return new DefaultProgramWorkflowRunner(cConf, workflowProgram, workflowProgramOptions, programRunnerFactory,
                                                  workflowSpec, token, nodeId, nodeStates, ProgramType.MAPREDUCE);
        case SPARK:
          return new DefaultProgramWorkflowRunner(cConf, workflowProgram, workflowProgramOptions, programRunnerFactory,
                                                  workflowSpec, token, nodeId, nodeStates, ProgramType.SPARK);
        default:
          LOG.debug("No workflow program runner found for this program");
      }
    } else {
      LOG.debug("ProgramType key not found in Workflow Action Specification Properties");
    }
    return null;  // if no workflow program runner was found for this program
  }
}
