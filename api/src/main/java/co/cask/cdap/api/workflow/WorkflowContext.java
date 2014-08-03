/*
 * Copyright 2014 Cask, Inc.
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
package co.cask.cdap.api.workflow;

import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;

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
