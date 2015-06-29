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
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import org.apache.twill.common.Threads;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * An Abstract class implementing {@link ProgramWorkflowRunner}, providing a {@link Runnable} of
 * the programs to run in a workflow.
 * <p>
 * Programs that extend this class (such as {@link MapReduceProgramWorkflowRunner} or
 * {@link SparkProgramWorkflowRunner}) can execute their associated programs through the
 * {@link AbstractProgramWorkflowRunner#executeProgram} by providing the {@link ProgramController} and
 * {@link RuntimeContext} that they obtained through the {@link ProgramRunner}.
 * </p>
 * The {@link RuntimeContext} is blocked until completion of the associated program.
 */
public abstract class AbstractProgramWorkflowRunner implements ProgramWorkflowRunner {
  private static final Gson GSON = new Gson();
  private final Arguments userArguments;
  private final Arguments systemArguments;
  private final String nodeId;
  protected final WorkflowSpecification workflowSpec;
  protected final ProgramRunnerFactory programRunnerFactory;
  protected final Program workflowProgram;
  protected final WorkflowToken token;

  public AbstractProgramWorkflowRunner(Program workflowProgram, ProgramOptions workflowProgramOptions,
                                       ProgramRunnerFactory programRunnerFactory, WorkflowSpecification workflowSpec,
                                       WorkflowToken token, String nodeId) {
    this.userArguments = workflowProgramOptions.getUserArguments();
    this.workflowProgram = workflowProgram;
    this.programRunnerFactory = programRunnerFactory;
    this.workflowSpec = workflowSpec;
    this.systemArguments = workflowProgramOptions.getArguments();
    this.token = token;
    this.nodeId = nodeId;
  }

  @Override
  public abstract Runnable create(String name);

  @Override
  public abstract void runAndWait(Program program, ProgramOptions options) throws Exception;

  /**
   * Gets a {@link Runnable} for the {@link Program}.
   *
   * @param name    name of the {@link Program}
   * @param program the {@link Program}
   * @return a {@link Runnable} for this {@link Program}
   */
  protected Runnable getProgramRunnable(String name, final Program program) {
    Map<String, String> systemArgumentsMap = Maps.newHashMap();
    systemArgumentsMap.putAll(systemArguments.asMap());
    // Generate the new RunId here for the program running under Workflow
    systemArgumentsMap.put(ProgramOptionConstants.RUN_ID, RunIds.generate().getId());

    // Add Workflow specific system arguments to be passed to the underlying program
    systemArgumentsMap.put(ProgramOptionConstants.WORKFLOW_NAME, workflowSpec.getName());
    systemArgumentsMap.put(ProgramOptionConstants.WORKFLOW_RUN_ID,
                           systemArguments.getOption(ProgramOptionConstants.RUN_ID));
    systemArgumentsMap.put(ProgramOptionConstants.WORKFLOW_NODE_ID, nodeId);
    systemArgumentsMap.put(ProgramOptionConstants.PROGRAM_NAME_IN_WORKFLOW, name);
    systemArgumentsMap.put(ProgramOptionConstants.WORKFLOW_TOKEN, GSON.toJson(token));

    final ProgramOptions options = new SimpleProgramOptions(
      program.getName(),
      new BasicArguments(ImmutableMap.copyOf(systemArgumentsMap)),
      new BasicArguments(RuntimeArguments.extractScope(Scope.scopeFor(program.getType().getCategoryName()), name,
                                                       userArguments.asMap()))
    );

    return new Runnable() {
      @Override
      public void run() {
        try {
          runAndWait(program, options);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  /**
   * Adds a listener to the {@link ProgramController} and blocks for completion.
   *
   * @param controller the {@link ProgramController} for the program
   * @param context    the {@link RuntimeContext}
   * @return {@link RuntimeContext} of the completed program
   * @throws Exception if the execution failed
   */
  protected RuntimeContext executeProgram(final ProgramController controller,
                                          final RuntimeContext context) throws Exception {
    // Execute the program.
    final SettableFuture<RuntimeContext> completion = SettableFuture.create();
    controller.addListener(new AbstractListener() {
      @Override
      public void completed() {
        completion.set(context);
      }

      @Override
      public void killed() {
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
    } catch (InterruptedException e) {
      try {
        Futures.getUnchecked(controller.stop());
      } catch (Throwable t) {
        // no-op
      }
      // reset the interrupt
      Thread.currentThread().interrupt();
      return null;
    }
  }
}
