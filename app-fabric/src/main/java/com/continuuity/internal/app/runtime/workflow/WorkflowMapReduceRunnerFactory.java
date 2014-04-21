/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.continuuity.internal.app.runtime.batch.MapReduceProgramController;
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
