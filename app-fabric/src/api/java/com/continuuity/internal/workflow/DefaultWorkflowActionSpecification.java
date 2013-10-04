/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.workflow;

import com.continuuity.api.workflow.WorkflowAction;
import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.internal.lang.Reflections;
import com.continuuity.internal.specification.PropertyFieldExtractor;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.util.Map;

/**
 *
 */
public class DefaultWorkflowActionSpecification implements WorkflowActionSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final Map<String, String> properties;

  public DefaultWorkflowActionSpecification(String name, String description, Map<String, String> properties) {
    this(null, name, description, properties);
  }

  public DefaultWorkflowActionSpecification(WorkflowAction action) {
    WorkflowActionSpecification spec = action.configure();

    Map<String, String> properties = Maps.newHashMap(spec.getProperties());
    Reflections.visit(action, TypeToken.of(action.getClass()),
                      new PropertyFieldExtractor(properties));

    this.className = action.getClass().getName();
    this.name = spec.getName();
    this.description = spec.getDescription();
    this.properties = ImmutableMap.copyOf(properties);
  }

  public DefaultWorkflowActionSpecification(String className, String name,
                                            String description, Map<String, String> properties) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.properties = ImmutableMap.copyOf(properties);
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
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(WorkflowActionSpecification.class)
      .add("name", name)
      .add("class", className)
      .add("options", properties)
      .toString();
  }
}
