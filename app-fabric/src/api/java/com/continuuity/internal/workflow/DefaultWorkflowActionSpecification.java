/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.workflow;

import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 *
 */
public class DefaultWorkflowActionSpecification implements WorkflowActionSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final Map<String, String> options;

  public DefaultWorkflowActionSpecification(String name, String description, Map<String, String> options) {
    this(null, name, description, options);
  }

  public DefaultWorkflowActionSpecification(String className, WorkflowActionSpecification spec) {
    this(className, spec.getName(), spec.getDescription(), spec.getOptions());
  }

  public DefaultWorkflowActionSpecification(String className, String name,
                                            String description, Map<String, String> options) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.options = ImmutableMap.copyOf(options);
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
  public Map<String, String> getOptions() {
    return options;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(WorkflowActionSpecification.class)
      .add("name", name)
      .add("class", className)
      .add("options", options)
      .toString();
  }
}
