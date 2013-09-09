/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

/**
 * Represents an action that can be executed in a {@link Workflow}. The lifecycle of a {@link WorkflowAction} is
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
   * Provides a specification about this {@link WorkflowAction}.
   *
   * @return An instance of {@link WorkflowSpecification}.
   */
  WorkflowActionSpecification configure();

  /**
   * Initializes a {@link WorkflowAction}. This method is called before the {@link #run()} method.
   *
   * @param context Context object containing runtime information for this action.
   * @throws Exception if there is any error during initialization. When exception is thrown, the execution of
   *         this action is treated as failure to the {@link Workflow}.
   *
   */
  void initialize(WorkflowContext context) throws Exception;

  /**
   * This method will get call after the {@link #run} method completed and it can be used for resource cleanup.
   * Any exception thrown will only get logged but does not affect execution of the {@link Workflow}.
   */
  void destroy();
}
