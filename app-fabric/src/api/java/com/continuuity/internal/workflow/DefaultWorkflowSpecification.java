/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.workflow;

import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 *
 */
public final class DefaultWorkflowSpecification implements WorkflowSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final List<WorkflowActionSpecification> actions;

  public DefaultWorkflowSpecification(String name, String description, List<WorkflowActionSpecification> actions) {
    this(null, name, description, actions);
  }

  public DefaultWorkflowSpecification(String className, WorkflowSpecification spec) {
    this(className, spec.getName(), spec.getDescription(), spec.getActions());
  }

  public DefaultWorkflowSpecification(String className, String name, String description,
                                      List<WorkflowActionSpecification> actions) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.actions = ImmutableList.copyOf(actions);
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public List<WorkflowActionSpecification> getActions() {
    return actions;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(WorkflowSpecification.class)
      .add("name", name)
      .add("class", className)
      .add("actions", actions)
      .toString();
  }
}
