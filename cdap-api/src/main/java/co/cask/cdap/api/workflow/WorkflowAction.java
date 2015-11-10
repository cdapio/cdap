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

/**
 * Represents an action that can be executed in a {@link Workflow}. The lifecycle of a {@link WorkflowAction} is:
 *
 * <pre>
 * try {
 *   {@link #initialize(WorkflowContext)}
 *   {@link #run()}
 *   // Success
 * } catch (Exception e) {
 *   // Failure
 * } finally {
 *   {@link #destroy()}
 * }
 * </pre>
 */
public interface WorkflowAction extends Runnable {

  /**
   * Provides a specification for this {@link WorkflowAction}.
   *
   * @return An instance of {@link WorkflowSpecification}.
   * @deprecated Use {@link AbstractWorkflowAction#configure} instead.
   */
  @Deprecated
  WorkflowActionSpecification configure();

  /**
   * Initializes a {@link WorkflowAction}. This method is called before the {@link #run()} method.
   *
   * @param context Context object containing runtime information for this action.
   * @throws Exception If there is any error during initialization. When an exception is thrown, the execution of
   *         this action is treated as failure of the {@link Workflow}.
   *
   */
  void initialize(WorkflowContext context) throws Exception;

  /**
   * This method is called after the {@link #run} method completes and it can be used for resource cleanup. 
   * Any exception thrown only gets logged but does not affect execution of the {@link Workflow}.
   */
  void destroy();
}
