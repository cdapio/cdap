/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

import com.continuuity.api.batch.MapReduce;

/**
 *
 */
public final class MapReduceWorkflowAction extends AbstractWorkflowAction {

  private final MapReduce mapReduce;

  public MapReduceWorkflowAction(MapReduce mapReduce) {
    this.mapReduce = mapReduce;
  }

  @Override
  public WorkflowActionSpecification configure() {
    String name = String.format("Action%s", mapReduce.configure().getName());

    return WorkflowActionSpecification.Builder.with()
      .setName(name)
      .setDescription(name)
      .build();
  }

  @Override
  public void run() {

  }
}
