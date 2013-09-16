/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.google.common.util.concurrent.Service;

/**
 * A container class for holding workflow status.
 */
final class WorkflowStatus {

  private final Service.State state;
  private final WorkflowActionSpecification currentAction;
  private final int currentStep;

  WorkflowStatus(Service.State state, WorkflowActionSpecification currentAction, int currentStep) {
    this.state = state;
    this.currentAction = currentAction;
    this.currentStep = currentStep;
  }
}
