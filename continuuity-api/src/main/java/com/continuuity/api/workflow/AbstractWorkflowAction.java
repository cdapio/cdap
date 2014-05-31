/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

/**
 *
 */
public abstract class AbstractWorkflowAction implements WorkflowAction {

  private final String name;
  private WorkflowContext context;

  protected AbstractWorkflowAction() {
    name = getClass().getSimpleName();
  }

  protected AbstractWorkflowAction(String name) {
    this.name = name;
  }

  @Override
  public WorkflowActionSpecification configure() {
    return WorkflowActionSpecification.Builder.with()
      .setName(getName())
      .setDescription(getDescription())
      .build();
  }

  @Override
  public void initialize(WorkflowContext context) throws Exception {
    this.context = context;
  }

  @Override
  public void destroy() {
    // No-op
  }

  protected final WorkflowContext getContext() {
    return context;
  }

  /**
   * @return {@link Class#getSimpleName() Simple classname} of this {@link WorkflowAction}.
   */
  protected String getName() {
    return name;
  }

  /**
   * @return A descriptive message about this {@link WorkflowAction}.
   */
  protected String getDescription() {
    return String.format("WorkFlowAction of %s.", getName());
  }
}
