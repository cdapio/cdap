/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

import com.continuuity.api.batch.MapReduceContext;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Represents runtime context of a {@link WorkflowAction}.
 */
public interface WorkflowContext {

  WorkflowSpecification getWorkflowSpecification();

  WorkflowActionSpecification getSpecification();

  long getLogicalStartTime();

  /**
   * Returns a {@link Callable} that will launch the {@link com.continuuity.api.batch.MapReduce} job
   * of the given name when the {@link Callable#call()}} method is called. When the MapReduce job is
   * completed, the {@link Callable} will return the {@link MapReduceContext} of the job or {@code null} if
   * no such context exists.
   * Exception will be thrown from the {@link Callable#call()} method if the MapReduce executed failed.
   *
   * @throws IllegalArgumentException if no MapReduce with the given name is defined in the workflow.
   */
  Callable<MapReduceContext> getMapReduceRunner(String name);

  /**
   * @return A map of argument key and value.
   */
  Map<String, String> getRuntimeArguments();
}
