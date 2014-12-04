/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import com.google.common.util.concurrent.Service;

/**
 * A ProgramController for MapReduce. It mainly is an adapter for reflecting the state changes in
 * {@link MapReduceRuntimeService}.
 */
public final class MapReduceProgramController extends ProgramControllerServiceAdapter {

  private final MapReduceContext context;

  MapReduceProgramController(Service mapReduceRuntimeService, BasicMapReduceContext context) {
    super(mapReduceRuntimeService, context.getProgramName(), context.getRunId());
    this.context = context;
  }

  @Override
  protected boolean propagateServiceError() {
    // Don't propagate MR failure as failure. Quick fix for CDAP-749.
    return false;
  }

  /**
   * Returns the {@link MapReduceContext} for MapReduce run represented by this controller.
   */
  public MapReduceContext getContext() {
    return context;
  }
}
