/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

/**
 *
 */
public interface WorkFlow {

  /**
   * Configures the {@link WorkFlow} by returning a {@link WorkFlowSpecification}.
   *
   * @return An instance of {@link WorkFlowSpecification}.
   */
  WorkFlowSpecification configure();
}
