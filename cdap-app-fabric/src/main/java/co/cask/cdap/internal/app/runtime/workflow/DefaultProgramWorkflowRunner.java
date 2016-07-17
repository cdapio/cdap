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
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.workflow.NodeStatus;
import co.cask.cdap.api.workflow.WorkflowNodeState;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.app.runtime.WorkflowTokenProvider;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramClassLoader;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import org.apache.twill.common.Threads;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 * A class implementing {@link ProgramWorkflowRunner}, providing a {@link Runnable} of
 * the program to run in a workflow.
 */
final class DefaultProgramWorkflowRunner implements ProgramWorkflowRunner {

  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final ProgramOptions workflowProgramOptions;
  private final Program workflowProgram;
  private final String nodeId;
  private final Map<String, WorkflowNodeState> nodeStates;
  private final WorkflowSpecification workflowSpec;
  private final ProgramRunnerFactory programRunnerFactory;
  private final WorkflowToken token;
  private final ProgramType programType;

  DefaultProgramWorkflowRunner(CConfiguration cConf, Program workflowProgram, ProgramOptions workflowProgramOptions,
                               ProgramRunnerFactory programRunnerFactory, WorkflowSpecification workflowSpec,
                               WorkflowToken token, String nodeId, Map<String, WorkflowNodeState> nodeStates,
                               ProgramType programType) {
    this.cConf = cConf;
    this.workflowProgram = workflowProgram;
    this.workflowProgramOptions = workflowProgramOptions;
    this.programRunnerFactory = programRunnerFactory;
    this.workflowSpec = workflowSpec;
    this.token = token;
    this.nodeId = nodeId;
    this.nodeStates = nodeStates;
    this.programType = programType;
  }

  @Override
  public Runnable create(String name) {
    ProgramRunner programRunner = programRunnerFactory.create(programType);
    try {
      Program program = createProgram(programRunner, name);
      return getProgramRunnable(name, programRunner, program);
    } catch (Exception e) {
      closeProgramRunner(programRunner);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Gets a {@link Runnable} for the {@link Program}.
   *
   * @param name    name of the {@link Program}
   * @param program the {@link Program}
   * @return a {@link Runnable} for this {@link Program}
   */
  private Runnable getProgramRunnable(String name, final ProgramRunner programRunner, final Program program) {
    Map<String, String> systemArgumentsMap = new HashMap<>(workflowProgramOptions.getArguments().asMap());

    // Generate the new RunId here for the program running under Workflow
    systemArgumentsMap.put(ProgramOptionConstants.RUN_ID, RunIds.generate().getId());

    // Add Workflow specific system arguments to be passed to the underlying program
    systemArgumentsMap.put(ProgramOptionConstants.WORKFLOW_NAME, workflowSpec.getName());
    systemArgumentsMap.put(ProgramOptionConstants.WORKFLOW_RUN_ID,
                           ProgramRunners.getRunId(workflowProgramOptions).getId());
    systemArgumentsMap.put(ProgramOptionConstants.WORKFLOW_NODE_ID, nodeId);
    systemArgumentsMap.put(ProgramOptionConstants.PROGRAM_NAME_IN_WORKFLOW, name);
    systemArgumentsMap.put(ProgramOptionConstants.WORKFLOW_TOKEN, GSON.toJson(token));

    final ProgramOptions options = new SimpleProgramOptions(
      program.getName(),
      new BasicArguments(Collections.unmodifiableMap(systemArgumentsMap)),
      new BasicArguments(RuntimeArguments.extractScope(Scope.scopeFor(program.getType().getCategoryName()), name,
                                                       workflowProgramOptions.getUserArguments().asMap()))
    );

    return new Runnable() {
      @Override
      public void run() {
        try {
          runAndWait(programRunner, program, options);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  /**
   * Creates a new {@link Program} instance for the execution by the given {@link ProgramRunner} and
   * the program name.
   */
  private Program createProgram(ProgramRunner programRunner, String programName) throws IOException {
    ClassLoader classLoader = workflowProgram.getClassLoader();
    // The classloader should be ProgramClassLoader
    Preconditions.checkArgument(classLoader instanceof ProgramClassLoader,
                                "Program %s doesn't use ProgramClassLoader", workflowProgram);

    ProgramId programId = new ProgramId(workflowProgram.getNamespaceId(), workflowProgram.getApplicationId(),
                                        programType, programName);
    ApplicationSpecification appSpec = workflowProgram.getApplicationSpecification();
    return Programs.create(cConf, programRunner, new ProgramDescriptor(programId, appSpec),
                           workflowProgram.getJarLocation(), ((ProgramClassLoader) classLoader).getDir());
  }

  private void runAndWait(ProgramRunner programRunner, Program program, ProgramOptions options) throws Exception {
    Closeable closeable = createCloseable(programRunner, program);
    ProgramController controller;
    try {
      controller = programRunner.run(program, options);
    } catch (Throwable t) {
      // If there is any exception when running the program, close the program to release resources.
      // Otherwise it will be released when the execution completed.
      Closeables.closeQuietly(closeable);
      throw t;
    }
    blockForCompletion(closeable, controller);

    if (controller instanceof WorkflowTokenProvider) {
      updateWorkflowToken(((WorkflowTokenProvider) controller).getWorkflowToken());
    } else {
      // This shouldn't happen
      throw new IllegalStateException("No WorkflowToken available after program completed: " + program.getId());
    }
  }

  /**
   * Adds a listener to the {@link ProgramController} and blocks for completion.
   *
   * @param closeable a {@link Closeable} to call when the program execution completed
   * @param controller the {@link ProgramController} for the program
   * @throws Exception if the execution failed
   */
  private void blockForCompletion(final Closeable closeable, final ProgramController controller) throws Exception {
    // Execute the program.
    final SettableFuture<Void> completion = SettableFuture.create();
    controller.addListener(new AbstractListener() {

      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        switch (currentState) {
          case COMPLETED:
            completed();
          break;
          case KILLED:
            killed();
          break;
          case ERROR:
            error(cause);
          break;
        }
      }

      @Override
      public void completed() {
        Closeables.closeQuietly(closeable);
        nodeStates.put(nodeId, new WorkflowNodeState(nodeId, NodeStatus.COMPLETED, controller.getRunId().getId(),
                                                     null));
        completion.set(null);
      }

      @Override
      public void killed() {
        Closeables.closeQuietly(closeable);
        nodeStates.put(nodeId, new WorkflowNodeState(nodeId, NodeStatus.KILLED, controller.getRunId().getId(), null));
        completion.set(null);
      }

      @Override
      public void error(Throwable cause) {
        Closeables.closeQuietly(closeable);
        nodeStates.put(nodeId, new WorkflowNodeState(nodeId, NodeStatus.FAILED, controller.getRunId().getId(), cause));
        completion.setException(cause);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    // Block for completion.
    try {
      completion.get();
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
    }
  }

  private void updateWorkflowToken(WorkflowToken workflowToken) throws Exception {
    ((BasicWorkflowToken) token).mergeToken(workflowToken);
  }

  /**
   * Creates a {@link Closeable} that will close the given {@link ProgramRunner} and {@link Program}.
   */
  private Closeable createCloseable(final ProgramRunner programRunner, final Program program) {
    return new Closeable() {
      @Override
      public void close() throws IOException {
        Closeables.closeQuietly(program);
        closeProgramRunner(programRunner);
      }
    };
  }

  /**
   * Closes the given {@link ProgramRunner} if it implements {@link Closeable}.
   */
  private void closeProgramRunner(ProgramRunner programRunner) {
    if (programRunner instanceof Closeable) {
      Closeables.closeQuietly((Closeable) programRunner);
    }
  }
}
