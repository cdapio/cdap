/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

import com.continuuity.api.batch.MapReduce;

/**
 *
 */
public final class MapReduceAction implements WorkFlowAction {

  private final MapReduce mapReduce;

  public MapReduceAction(MapReduce mapReduce) {
    this.mapReduce = mapReduce;
  }

  @Override
  public void initialize(WorkFlowContext context) throws Exception {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void run() {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void destroy() {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
