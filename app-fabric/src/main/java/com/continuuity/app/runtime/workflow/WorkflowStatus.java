/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.runtime.workflow;

import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.google.common.util.concurrent.Service;

/**
 * A container class for holding workflow status.
 */
public final class WorkflowStatus {

  private  Service.State state;
  private  WorkflowActionSpecification currentAction;
  private  int currentStep;

  public WorkflowStatus(Service.State state, WorkflowActionSpecification currentAction, int currentStep) {
    this.state = state;
    this.currentAction = currentAction;
    this.currentStep = currentStep;
  }

  public Service.State getState() {
    return state;
  }

  public WorkflowActionSpecification getCurrentAction() {
    return currentAction;
  }
}
