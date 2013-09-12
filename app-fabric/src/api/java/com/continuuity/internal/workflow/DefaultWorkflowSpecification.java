/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.workflow;

import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 *
 */
public final class DefaultWorkflowSpecification implements WorkflowSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final List<WorkflowActionSpecification> actions;
  private final Map<String, MapReduceSpecification> mapReduces;

  public DefaultWorkflowSpecification(String name, String description, List<WorkflowActionSpecification> actions,
                                      Map<String, MapReduceSpecification> mapReduces) {
    this(null, name, description, actions, mapReduces);
  }

  public DefaultWorkflowSpecification(String className, WorkflowSpecification spec) {
    this(className, spec.getName(), spec.getDescription(), spec.getActions(), spec.getMapReduces());
  }

  public DefaultWorkflowSpecification(String className, String name, String description,
                                      List<WorkflowActionSpecification> actions,
                                      Map<String, MapReduceSpecification> mapReduces) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.actions = ImmutableList.copyOf(actions);
    this.mapReduces = ImmutableMap.copyOf(mapReduces);
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
  public Map<String, MapReduceSpecification> getMapReduces() {
    return mapReduces;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(WorkflowSpecification.class)
      .add("name", name)
      .add("class", className)
      .add("actions", actions)
      .add("mapReduces", mapReduces)
      .toString();
  }
}
