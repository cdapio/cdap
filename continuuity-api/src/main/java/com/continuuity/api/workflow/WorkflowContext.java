/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

import com.continuuity.api.mapreduce.MapReduce;
import com.continuuity.api.mapreduce.MapReduceContext;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Represents the runtime context of a {@link WorkflowAction}.
 */
public interface WorkflowContext {

  /**
   * 
   */
  WorkflowSpecification getWorkflowSpecification();

  WorkflowActionSpecification getSpecification();

  long getLogicalStartTime();

  /**
   * Returns a {@link Callable} that launches the {@link MapReduce} job
   * of the specified name when the {@link Callable#call()} method is called. When the MapReduce job completes, 
   * the {@link Callable} returns the {@link MapReduceContext} of the job or {@code null} if
   * no such context exists.
   *
   * An Exception is thrown from the {@link Callable#call()} method if the MapReduce job fails.
   *
   * @throws IllegalArgumentException if no MapReduce job with the specified name is defined in the workflow.
   */
  Callable<MapReduceContext> getMapReduceRunner(String name);

  /**
   * @return A map of the argument's key and value.
   */
  Map<String, String> getRuntimeArguments();
}
