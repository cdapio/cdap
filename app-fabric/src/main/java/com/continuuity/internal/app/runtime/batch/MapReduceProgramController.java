/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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
