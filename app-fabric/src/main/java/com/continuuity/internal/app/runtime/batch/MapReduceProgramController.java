/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.internal.app.runtime.AbstractProgramController;

/**
 *
 */
public final class MapReduceProgramController extends AbstractProgramController {

  private final MapReduceContext context;

  MapReduceProgramController(BasicMapReduceContext context) {
    super(context.getProgramName(), context.getRunId());
    this.context = context;
    started();
  }

  @Override
  protected void doSuspend() throws Exception {
    // No-op
  }

  @Override
  protected void doResume() throws Exception {
    // No-op
  }

  @Override
  protected void doStop() throws Exception {
    // When job is stopped by controller doStop() method, the stopping() method of listener is also called.
    // That is where we kill the job, so no need to do any extra job in doStop().
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    // No-op
  }

  /**
   * Returns the {@link MapReduceContext} for MapReduce run represented by this controller.
   */
  public MapReduceContext getContext() {
    return context;
  }
}
