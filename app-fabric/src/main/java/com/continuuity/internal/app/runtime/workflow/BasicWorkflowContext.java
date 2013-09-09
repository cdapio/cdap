/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.api.workflow.WorkflowContext;
import com.continuuity.api.workflow.WorkflowSpecification;

/**
 *
 */
final class BasicWorkflowContext implements WorkflowContext {

  private final WorkflowSpecification specification;

  BasicWorkflowContext(WorkflowSpecification specification) {
    this.specification = specification;
  }

  @Override
  public WorkflowSpecification getSpecification() {
    return specification;
  }
}
