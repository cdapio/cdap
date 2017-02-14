/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.realtime;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.common.AbstractTransformContext;
import co.cask.cdap.etl.planner.StageInfo;

/**
 * Implementation of {@link RealtimeContext} for {@link Worker} driver.
 */
public class WorkerRealtimeContext extends AbstractTransformContext implements RealtimeContext {
  private final WorkerContext context;

  public WorkerRealtimeContext(WorkerContext context, Metrics metrics, LookupProvider lookup, StageInfo stageInfo) {
    super(context, metrics, lookup, stageInfo);
    this.context = context;
  }

  @Override
  public int getInstanceId() {
    return context.getInstanceId();
  }

  @Override
  public int getInstanceCount() {
    return context.getInstanceCount();
  }

}
