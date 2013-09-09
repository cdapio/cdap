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
    return super.configure();    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public void run() {

  }
}
