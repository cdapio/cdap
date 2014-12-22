/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.RuntimeContext;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Represents the runtime context of a {@link WorkflowAction}.
 */
public interface WorkflowContext {

  WorkflowSpecification getWorkflowSpecification();

  WorkflowActionSpecification getSpecification();

  long getLogicalStartTime();

  /**
   * Returns a {@link Callable} that launches the associated program
   * with the specified name when the {@link Callable#call() call} method is invoked. When the program completes,
   * the {@link Callable} returns the {@link RuntimeContext} of the program or {@code null} if
   * no such context exists.
   * <p/>
   * <p> An Exception is thrown from the {@link Callable#call()} method if the program fails </p>
   *
   * @throws IllegalArgumentException if no program with the specified name is defined in the workflow
   */
  Callable<RuntimeContext> getProgramRunner(String name);

  /**
   * @return A map of the argument's key and value.
   */
  Map<String, String> getRuntimeArguments();
}
