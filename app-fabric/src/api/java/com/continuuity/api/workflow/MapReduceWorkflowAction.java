/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

import com.continuuity.api.batch.MapReduce;
import com.google.common.collect.ImmutableMap;

/**
 *
 */
public final class MapReduceWorkflowAction extends AbstractWorkflowAction {

  private static final String MAPREDUCE_CLASS = "mrClass";

  private MapReduce mapReduce;

  public MapReduceWorkflowAction(MapReduce mapReduce) {
    this.mapReduce = mapReduce;
  }

  @Override
  public WorkflowActionSpecification configure() {
    String name = String.format("Workflow:%s", mapReduce.configure().getName());

    return WorkflowActionSpecification.Builder.with()
      .setName(name)
      .setDescription(name)
      .withOptions(ImmutableMap.of(MAPREDUCE_CLASS, mapReduce.getClass().getName()))
      .build();
  }

  @Override
  public void run() {
    getContext().getSpecification().getOptions().get(MAPREDUCE_CLASS);
  }
}
