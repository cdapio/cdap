/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

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
   */
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
