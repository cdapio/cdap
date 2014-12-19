/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.batch.MapReduceProgramController;
import com.google.common.base.Preconditions;
import org.apache.twill.api.RunId;

import java.util.concurrent.Callable;

/**
 * A {@link ProgramWorkflowRunner} that creates {@link Callable} for executing MapReduce job from Workflow.
 */
final class MapReduceProgramWorkflowRunner extends AbstractProgramWorkflowRunner {

  MapReduceProgramWorkflowRunner(WorkflowSpecification workflowSpec, ProgramRunnerFactory programRunnerFactory,
                                 Program workflowProgram, RunId runId,
                                 Arguments userArguments, long logicalStartTime) {
    super(userArguments, runId, workflowProgram, logicalStartTime, programRunnerFactory, workflowSpec);
  }

  /**
   * Gets the Specification of the program by its name from the {@link WorkflowSpecification}. Creates an
   * appropriate {@link Program} using this specification through a suitable concrete implementation of
   * * {@link AbstractWorkflowProgram} and then gets the {@link Callable} of {@link RuntimeContext} for the program
   * which can be called to execute the program
   *
   * @param name name of the program in the workflow
   * @return {@link Callable} of {@link RuntimeContext} for associated with this program run.
   */
  @Override
  public Callable<RuntimeContext> create(String name) {

    final MapReduceSpecification mapReduceSpec = workflowSpec.getMapReduce().get(name);
    Preconditions.checkArgument(mapReduceSpec != null,
                                "No MapReduce with name %s found in Workflow %s", name, workflowSpec.getName());

    final Program mapReduceProgram = new WorkflowMapReduceProgram(workflowProgram, mapReduceSpec);
    return getRuntimeContextCallable(name, mapReduceProgram);
  }

  /**
   * Executes given {@link Program} with the given {@link ProgramOptions} and block until it completed. 
   * On completion, return the {@link RuntimeContext} of the program.
   *
   * @throws Exception if execution failed.
   */
  @Override
  public RuntimeContext runAndWait(Program program, ProgramOptions options) throws Exception {
    ProgramController controller = programRunnerFactory.create(ProgramRunnerFactory.Type.MAPREDUCE).run(program,
                                                                                                        options);
    final RuntimeContext context = (controller instanceof MapReduceProgramController)
      ? ((MapReduceProgramController) controller).getContext()
      : null;
    return executeProgram(controller, context);

  }

}
