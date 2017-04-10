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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.batch.AbstractBatchContext;
import co.cask.cdap.etl.log.LogContext;
import co.cask.cdap.etl.planner.StageInfo;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Abstract implementation of {@link BatchContext} using {@link MapReduceContext}.
 */
public class MapReduceBatchContext extends AbstractBatchContext {
  protected final MapReduceContext mrContext;

  public MapReduceBatchContext(MapReduceContext context, Metrics metrics,
                               LookupProvider lookup, Map<String, String> runtimeArguments,
                               StageInfo stageInfo) {
    super(context, context, metrics, lookup, context.getLogicalStartTime(), runtimeArguments, context.getAdmin(),
          stageInfo);
    this.mrContext = context;
  }

  @Override
  public <T> T getHadoopJob() {
    return LogContext.runWithoutLoggingUnchecked(new Callable<T>() {
      @Override
      public T call() throws Exception {
        return mrContext.getHadoopJob();
      }
    });
  }
}
