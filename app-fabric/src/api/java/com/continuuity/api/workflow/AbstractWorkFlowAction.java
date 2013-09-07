/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

/**
 *
 */
public abstract class AbstractWorkFlowAction implements WorkFlowAction {

  private final String name;
  private WorkFlowContext context;

  protected AbstractWorkFlowAction() {
    name = getClass().getSimpleName();
  }

  protected AbstractWorkFlowAction(String name) {
    this.name = name;
  }

  @Override
  public void initialize(WorkFlowContext context) throws Exception {
    this.context = context;
  }

  @Override
  public void destroy() {
    // No-op
  }

  protected final WorkFlowContext getContext() {
    return context;
  }
}
