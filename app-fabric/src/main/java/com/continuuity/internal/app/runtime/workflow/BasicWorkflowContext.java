/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.api.batch.MapReduceContext;
import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.api.workflow.WorkflowContext;
import com.continuuity.api.workflow.WorkflowSpecification;

import java.util.concurrent.Callable;

/**
 *
 */
final class BasicWorkflowContext implements WorkflowContext {

  private final WorkflowSpecification workflowSpec;
  private final WorkflowActionSpecification specification;
  private final long logicalStartTime;
  private final MapReduceRunnerFactory runnerFactory;

  BasicWorkflowContext(WorkflowSpecification workflowSpec, WorkflowActionSpecification specification,
                       long logicalStartTime, MapReduceRunnerFactory runnerFactory) {
    this.workflowSpec = workflowSpec;
    this.specification = specification;
    this.logicalStartTime = logicalStartTime;
    this.runnerFactory = runnerFactory;
  }

  @Override
  public WorkflowSpecification getWorkflowSpecification() {
    return workflowSpec;
  }

  @Override
  public WorkflowActionSpecification getSpecification() {
    return specification;
  }

  @Override
  public long getLogicalStartTime() {
    return logicalStartTime;
  }

  @Override
  public Callable<MapReduceContext> getMapReduceRunner(String name) {
    return runnerFactory.create(name);
  }
}
