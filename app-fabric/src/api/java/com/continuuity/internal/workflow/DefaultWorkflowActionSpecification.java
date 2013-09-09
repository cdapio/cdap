/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.workflow;

import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.google.common.base.Objects;

/**
 *
 */
public final class DefaultWorkflowActionSpecification implements WorkflowActionSpecification {

  private final String className;
  private final String name;
  private final String description;

  public DefaultWorkflowActionSpecification(String name, String description) {
    this(null, name, description);
  }

  public DefaultWorkflowActionSpecification(String className, WorkflowActionSpecification spec) {
    this(className, spec.getName(), spec.getDescription());
  }

  public DefaultWorkflowActionSpecification(String className, String name, String description) {
    this.className = className;
    this.name = name;
    this.description = description;
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
  public String toString() {
    return Objects.toStringHelper(WorkflowActionSpecification.class)
      .add("name", name)
      .add("class", className)
      .toString();
  }
}
