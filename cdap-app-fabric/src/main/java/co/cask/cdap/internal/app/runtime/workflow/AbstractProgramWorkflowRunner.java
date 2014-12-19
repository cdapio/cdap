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
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * An Abstract class which implements {@link ProgramWorkflowRunner} and provides functionality to programs which can run
 * in workflow to get a {@link Callable} of {@link RuntimeContext} of that Program and then these program which
 * extend this class (like {@link MapReduceProgramWorkflowRunner} and {@link SparkProgramWorkflowRunner}) can execute
 * their associated  program through {@link AbstractProgramWorkflowRunner#executeProgram} by providing their
 * {@link ProgramController} and {@link RuntimeContext} obtained through the {@link ProgramRunner}.
 * * This {@link RuntimeContext} is also blocked upon till the completion of the program.
 */
public abstract class AbstractProgramWorkflowRunner implements ProgramWorkflowRunner {
  protected final WorkflowSpecification workflowSpec;
  protected final ProgramRunnerFactory programRunnerFactory;
  protected final Program workflowProgram;
  private final RunId runId;
  private final Arguments userArguments;
  private final long logicalStartTime;

  public AbstractProgramWorkflowRunner(Arguments userArguments, RunId runId, Program workflowProgram,
                                       long logicalStartTime, ProgramRunnerFactory programRunnerFactory,
                                       WorkflowSpecification workflowSpec) {
    this.userArguments = userArguments;
    this.runId = runId;
    this.workflowProgram = workflowProgram;
    this.logicalStartTime = logicalStartTime;
    this.programRunnerFactory = programRunnerFactory;
    this.workflowSpec = workflowSpec;
  }

  @Override
  public abstract Callable<RuntimeContext> create(String name);

  @Override
  public abstract RuntimeContext runAndWait(Program program, ProgramOptions options) throws Exception;

  protected Callable<RuntimeContext> getRuntimeContextCallable(String name, final Program program) {
    final ProgramOptions options = new SimpleProgramOptions(
      program.getName(),
      new BasicArguments(ImmutableMap.of(
        ProgramOptionConstants.RUN_ID, runId.getId(),
        ProgramOptionConstants.LOGICAL_START_TIME, Long.toString(logicalStartTime),
        ProgramOptionConstants.WORKFLOW_BATCH, name
      )),
      userArguments
    );

    return new Callable<RuntimeContext>() {
      @Override
      public RuntimeContext call() throws Exception {
        return runAndWait(program, options);
      }
    };
  }

  protected RuntimeContext executeProgram(ProgramController controller, final RuntimeContext context) throws Exception {
    // Execute the program.
    final SettableFuture<RuntimeContext> completion = SettableFuture.create();
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
