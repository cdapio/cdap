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

package io.cdap.cdap.internal.app.runtime.workflow;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.workflow.NodeStatus;
import io.cdap.cdap.api.workflow.WorkflowNodeState;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.program.Programs;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.runtime.WorkflowDataProvider;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
  private final ProgramStateWriter programStateWriter;

  DefaultProgramWorkflowRunner(CConfiguration cConf, Program workflowProgram, ProgramOptions workflowProgramOptions,
                               ProgramRunnerFactory programRunnerFactory, WorkflowSpecification workflowSpec,
                               WorkflowToken token, String nodeId, Map<String, WorkflowNodeState> nodeStates,
                               ProgramType programType, ProgramStateWriter programStateWriter) {
    this.cConf = cConf;
    this.workflowProgram = workflowProgram;
    this.workflowProgramOptions = workflowProgramOptions;
    this.programRunnerFactory = programRunnerFactory;
    this.workflowSpec = workflowSpec;
    this.token = token;
    this.nodeId = nodeId;
    this.nodeStates = nodeStates;
    this.programType = programType;
    this.programStateWriter = programStateWriter;
  }

  @Override
  public Runnable create(String name) {
    ProgramRunner programRunner = programRunnerFactory.create(programType);
    try {
      ProgramId programId = workflowProgram.getId().getParent().program(programType, name);
      Program program = Programs.create(cConf, workflowProgram, programId, programRunner);
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
      program.getId(),
      new BasicArguments(Collections.unmodifiableMap(systemArgumentsMap)),
      new BasicArguments(RuntimeArguments.extractScope(program.getType().getScope(), name,
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

  private void runAndWait(ProgramRunner programRunner, Program program, ProgramOptions options) throws Exception {
    Closeable closeable = createCloseable(programRunner, program);

    // Publish the program's starting state
    RunId runId = ProgramRunners.getRunId(options);
    String twillRunId = options.getArguments().getOption(ProgramOptionConstants.TWILL_RUN_ID);
    ProgramDescriptor programDescriptor = new ProgramDescriptor(program.getId(), program.getApplicationSpecification());
    programStateWriter.start(program.getId().run(runId), options, twillRunId, programDescriptor);

    ProgramController controller;
    try {
      controller = programRunner.run(program, options);
    } catch (Throwable t) {
      // If there is any exception when running the program, close the program to release resources.
      // Otherwise it will be released when the execution completed.
      programStateWriter.error(program.getId().run(runId), t);
      Closeables.closeQuietly(closeable);
      throw t;
    }
    blockForCompletion(closeable, controller);

    if (controller instanceof WorkflowDataProvider) {
      updateWorkflowToken(((WorkflowDataProvider) controller).getWorkflowToken());
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
        Set<Operation> fieldLineageOperations = new HashSet<>();
        if (controller instanceof WorkflowDataProvider) {
          fieldLineageOperations.addAll(((WorkflowDataProvider) controller).getFieldLineageOperations());
        }
        nodeStates.put(nodeId, new WorkflowNodeState(nodeId, NodeStatus.COMPLETED, fieldLineageOperations,
                                                     controller.getRunId().getId(), null));
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
