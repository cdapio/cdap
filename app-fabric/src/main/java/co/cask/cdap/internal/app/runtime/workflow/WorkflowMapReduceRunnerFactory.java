/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.batch.MapReduceProgramController;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * A {@link MapReduceRunnerFactory} that creates {@link Callable} for executing MapReduce job from Workflow.
 */
final class WorkflowMapReduceRunnerFactory implements MapReduceRunnerFactory {

  private final WorkflowSpecification workflowSpec;
  private final ProgramRunner programRunner;
  private final Program workflowProgram;
  private final RunId runId;
  private final Arguments userArguments;
  private final long logicalStartTime;

  WorkflowMapReduceRunnerFactory(WorkflowSpecification workflowSpec, ProgramRunner programRunner,
                                 Program workflowProgram, RunId runId,
                                 Arguments userArguments, long logicalStartTime) {
    this.workflowSpec = workflowSpec;
    this.programRunner = programRunner;
    this.workflowProgram = workflowProgram;
    this.runId = runId;
    this.logicalStartTime = logicalStartTime;
    this.userArguments = userArguments;
  }

  @Override
  public Callable<MapReduceContext> create(String name) {

    final MapReduceSpecification mapReduceSpec = workflowSpec.getMapReduce().get(name);
    Preconditions.checkArgument(mapReduceSpec != null,
                                "No MapReduce with name %s found in Workflow %s", name, workflowSpec.getName());

    final Program mapReduceProgram = new WorkflowMapReduceProgram(workflowProgram, mapReduceSpec);
    final ProgramOptions options = new SimpleProgramOptions(
      mapReduceProgram.getName(),
      new BasicArguments(ImmutableMap.of(
        ProgramOptionConstants.RUN_ID, runId.getId(),
        ProgramOptionConstants.LOGICAL_START_TIME, Long.toString(logicalStartTime),
        ProgramOptionConstants.WORKFLOW_BATCH, name
      )),
      userArguments
    );

    return new Callable<MapReduceContext>() {
      @Override
      public MapReduceContext call() throws Exception {
        return runAndWait(mapReduceProgram, options);
      }
    };
  }

  /**
   * Executes given MapReduce Program and block until it completed. On completion, return the MapReduceContext.
   *
   * @throws Exception if execution failed.
   */
  private MapReduceContext runAndWait(Program program, ProgramOptions options) throws Exception {
    ProgramController controller = programRunner.run(program, options);
    final MapReduceContext context = (controller instanceof MapReduceProgramController)
                                        ? ((MapReduceProgramController) controller).getContext()
                                        : null;
    // Execute the program.
    final SettableFuture<MapReduceContext> completion = SettableFuture.create();
    controller.addListener(new AbstractListener() {
      @Override
      public void stopped() {
        completion.set(context);
      }

      @Override
      public void error(Throwable cause) {
        completion.setException(cause);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    // Block for completion.
    try {
      return completion.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw Throwables.propagate(cause);
    }
  }
}
