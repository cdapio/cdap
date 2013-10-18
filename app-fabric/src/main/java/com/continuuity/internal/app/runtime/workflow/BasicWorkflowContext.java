/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.api.workflow.WorkflowContext;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 *
 */
final class BasicWorkflowContext implements WorkflowContext {

  private final WorkflowSpecification workflowSpec;
  private final WorkflowActionSpecification specification;
  private final long logicalStartTime;
  private final MapReduceRunnerFactory runnerFactory;
  private final Map<String, String> runtimeArgs;

  BasicWorkflowContext(WorkflowSpecification workflowSpec, WorkflowActionSpecification specification,
                       long logicalStartTime, MapReduceRunnerFactory runnerFactory, Map<String, String> runtimeArgs) {
    this.workflowSpec = workflowSpec;
    this.specification = specification;
    this.logicalStartTime = logicalStartTime;
    this.runnerFactory = runnerFactory;
    this.runtimeArgs = ImmutableMap.copyOf(runtimeArgs);
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

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArgs;
  }
}
