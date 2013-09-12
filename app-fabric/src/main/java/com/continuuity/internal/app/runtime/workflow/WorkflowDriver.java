/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.api.workflow.WorkflowAction;
import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.internal.app.runtime.batch.MapReduceProgramRunner;
import com.continuuity.internal.io.InstantiatorFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Core of Workflow engine that drives the execution of Workflow.
 */
final class WorkflowDriver extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowDriver.class);
  private static final String LOGICAL_START_TIME = "logicalStartTime";

  private final Program program;
  private final WorkflowSpecification workflowSpec;
  private final long logicalStartTime;
  private final MapReduceRunnerFactory runnerFactory;
  private volatile boolean running;

  WorkflowDriver(Program program, ProgramOptions options,
                 WorkflowSpecification workflowSpec, MapReduceProgramRunner programRunner) {
    this.program = program;
    this.workflowSpec = workflowSpec;
    this.logicalStartTime = options.getArguments().hasOption(LOGICAL_START_TIME)
                                ? Long.parseLong(options.getArguments().getOption(LOGICAL_START_TIME))
                                : System.currentTimeMillis();

    this.runnerFactory = new WorkflowMapReduceRunnerFactory(workflowSpec, programRunner, program, logicalStartTime);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Workflow {}", workflowSpec);
    running = true;
  }

  @Override
  protected void run() throws Exception {
    LOG.info("Start workflow execution for {}", workflowSpec);
    InstantiatorFactory instantiator = new InstantiatorFactory(false);
    ClassLoader classLoader = program.getClassLoader();

    Iterator<WorkflowActionSpecification> iterator = workflowSpec.getActions().iterator();
    while (running && iterator.hasNext()) {
      WorkflowActionSpecification actionSpec = iterator.next();
      WorkflowAction action = initialize(actionSpec, classLoader, instantiator);

      try {
        action.run();
      } catch (Throwable t) {
        LOG.warn("Exception on WorkflowAction.run(), aborting Workflow. {}", actionSpec);
        // this will always rethrow
        Throwables.propagateIfPossible(t, Exception.class);
      } finally {
        // Destroy the action.
        destroy(actionSpec, action);
      }
    }

    // If there is some task left when the loop exited, it must be called by explicit stop of this driver.
    if (iterator.hasNext()) {
      LOG.warn("Workflow explicitly stopped. Treated as abort on error. {} {}", workflowSpec);
      throw new IllegalStateException("Workflow stopped without executing all tasks: " + workflowSpec);
    }

    LOG.info("Workflow execution succeeded for {}", workflowSpec);

    running = false;
  }

  @Override
  protected void triggerShutdown() {
    running = false;
  }

  private WorkflowAction initialize(WorkflowActionSpecification actionSpec,
                                    ClassLoader classLoader, InstantiatorFactory instantiator) throws Exception {
    Class<?> clz = Class.forName(actionSpec.getClassName(), true, classLoader);
    Preconditions.checkArgument(WorkflowAction.class.isAssignableFrom(clz), "%s is not a WorkflowAction.", clz);
    WorkflowAction action = instantiator.get(TypeToken.of((Class<? extends WorkflowAction>) clz)).create();

    try {
      action.initialize(new BasicWorkflowContext(workflowSpec, actionSpec, logicalStartTime, runnerFactory));
    } catch (Throwable t) {
      LOG.warn("Exception on WorkflowAction.initialize(), abort Workflow. {}", actionSpec, t);
      // this will always rethrow
      Throwables.propagateIfPossible(t, Exception.class);
    }

    return action;
  }

  private void destroy(WorkflowActionSpecification actionSpec, WorkflowAction action) {
    try {
      action.destroy();
    } catch (Throwable t) {
      LOG.warn("Exception on WorkflowAction.destroy(): {}", actionSpec, t);
      // Just log, but not propagate
    }
  }
}
