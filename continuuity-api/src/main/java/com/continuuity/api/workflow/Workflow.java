/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

/**
 * Configures the {@link Workflow} by returning a {@link WorkflowSpecification}.
 */
public interface Workflow {

  /**
   * Configures the {@link Workflow} by returning a {@link WorkflowSpecification}.
   *
   * @return An instance of {@link WorkflowSpecification}.
   */
  WorkflowSpecification configure();
}
