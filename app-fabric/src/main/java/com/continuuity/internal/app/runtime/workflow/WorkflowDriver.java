/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.api.workflow.WorkflowAction;
import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.api.workflow.WorkflowContext;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.internal.io.InstantiatorFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * Core of Workflow engine that drives the execution of Workflow.
 */
public final class WorkflowDriver extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowDriver.class);

  private final Program program;
  private final WorkflowSpecification workflowSpec;
  private final List<ActionInfo> actions;
  private volatile boolean running;

  public WorkflowDriver(Program program, WorkflowSpecification workflowSpec) {
    this.program = program;
    this.workflowSpec = workflowSpec;
    this.actions = Lists.newArrayList();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Workflow {}", workflowSpec);

    InstantiatorFactory instantiator = new InstantiatorFactory(false);
    WorkflowContext context = new BasicWorkflowContext(workflowSpec);

    ClassLoader classLoader = program.getClassLoader();
    for (WorkflowActionSpecification actionSpec : workflowSpec.getActions()) {
      Class<?> clz = Class.forName(actionSpec.getClassName(), true, classLoader);
      Preconditions.checkArgument(WorkflowAction.class.isAssignableFrom(clz), "%s is not a WorkflowAction.", clz);
      WorkflowAction action = instantiator.get(TypeToken.of((Class<? extends WorkflowAction>) clz)).create();

      try {
        action.initialize(context);
      } catch (Throwable t) {
        LOG.warn("Exception on WorkflowAction.initialize(), abort Workflow. {}", actionSpec, t);
        // this will always rethrow
        Throwables.propagateIfPossible(t, Exception.class);
      }
      actions.add(new ActionInfo(actionSpec, action));
    }

    running = true;
  }

  @Override
  protected void run() throws Exception {
    LOG.info("Start workflow execution for {}", workflowSpec);
    Iterator<ActionInfo> iterator = actions.iterator();
    while (running && iterator.hasNext()) {
      ActionInfo actionInfo = iterator.next();
      try {
        actionInfo.getAction().run();
      } catch (Throwable t) {
        LOG.warn("Exception on WorkflowAction.run(), aborting Workflow. {}", actionInfo.getSpec());
        // this will always rethrow
        Throwables.propagateIfPossible(t, Exception.class);
      }
    }

    // If there is some task left when the loop exited, it must be called by explicit stop of this driver.
    if (iterator.hasNext()) {
      LOG.warn("Workflow explicitly stopped. Treated as abort on error. {} {}", workflowSpec);
      throw new IllegalStateException("Workflow stopped without executing all tasks: " + workflowSpec);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    for (ActionInfo action : actions) {
      try {
        action.getAction().destroy();
      } catch (Throwable t) {
        LOG.warn("Exception on WorkflowAction.destroy(): {}", action.getSpec(), t);
      }
    }
  }

  @Override
  protected void triggerShutdown() {
    running = false;
  }

  private static final class ActionInfo {
    private final WorkflowActionSpecification spec;
    private final WorkflowAction action;

    ActionInfo(WorkflowActionSpecification spec, WorkflowAction action) {
      this.spec = spec;
      this.action = action;
    }

    WorkflowActionSpecification getSpec() {
      return spec;
    }

    WorkflowAction getAction() {
      return action;
    }
  }
}
