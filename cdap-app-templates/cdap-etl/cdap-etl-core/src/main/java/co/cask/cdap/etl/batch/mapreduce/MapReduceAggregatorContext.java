/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.batch.AbstractAggregatorContext;
import co.cask.cdap.etl.planner.StageInfo;

import java.util.Map;

/**
 * MapReduce Aggregator Context.
 */
public class MapReduceAggregatorContext extends AbstractAggregatorContext {
  private final MapReduceContext mrContext;

  public MapReduceAggregatorContext(MapReduceContext context, Metrics metrics, LookupProvider lookup,
                                    Map<String, String> runtimeArgs, StageInfo stageInfo) {
    super(context, context, context, metrics, lookup, context.getLogicalStartTime(),
          runtimeArgs, context.getAdmin(), stageInfo);
    this.mrContext = context;
  }

  @Override
  public <T> T getHadoopJob() {
    return mrContext.getHadoopJob();
  }
}
