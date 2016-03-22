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

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import com.google.common.base.Preconditions;

/**
 * A {@link ProgramWorkflowRunner} that creates {@link Runnable} for executing MapReduce job from Workflow.
 */
final class MapReduceProgramWorkflowRunner extends AbstractProgramWorkflowRunner {

  MapReduceProgramWorkflowRunner(WorkflowSpecification workflowSpec, ProgramRunnerFactory programRunnerFactory,
                                 Program workflowProgram, ProgramOptions workflowProgramOptions, WorkflowToken token,
                                 String nodeId) {
    super(workflowProgram, workflowProgramOptions, programRunnerFactory, workflowSpec, token, nodeId);
  }

  /**
   * Gets the Specification of the program by its name from the {@link WorkflowSpecification}. Creates an
   * appropriate {@link Program} using this specification through a suitable concrete implementation of
   * * {@link AbstractWorkflowProgram} and then gets the {@link Runnable} for the program
   * which can be called to execute the program
   *
   * @param name name of the program in the workflow
   * @return {@link Runnable} associated with this program run.
   */
  @Override
  public Runnable create(String name) {
    ApplicationSpecification spec = workflowProgram.getApplicationSpecification();
    final MapReduceSpecification mapReduceSpec = spec.getMapReduce().get(name);
    Preconditions.checkArgument(mapReduceSpec != null,
                                "No MapReduce with name %s found in Application %s", name, spec.getName());

    final Program mapReduceProgram = new WorkflowMapReduceProgram(workflowProgram, mapReduceSpec);
    return getProgramRunnable(name, mapReduceProgram);
  }
}
