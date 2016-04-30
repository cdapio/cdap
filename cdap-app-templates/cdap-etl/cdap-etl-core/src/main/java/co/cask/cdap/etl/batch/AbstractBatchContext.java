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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.common.AbstractTransformContext;
import co.cask.cdap.etl.log.LogContext;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * MapReduce Aggregator Context.
 */
public abstract class AbstractBatchContext extends AbstractTransformContext implements BatchContext {
  private final DatasetContext datasetContext;
  private final long logicalStartTime;
  private final Map<String, String> runtimeArgs;

  protected AbstractBatchContext(PluginContext pluginContext,
                                 DatasetContext datasetContext,
                                 Metrics metrics,
                                 LookupProvider lookup,
                                 String stageName,
                                 long logicalStartTime,
                                 Map<String, String> runtimeArgs) {
    super(pluginContext, metrics, lookup, stageName);
    this.datasetContext = datasetContext;
    this.logicalStartTime = logicalStartTime;
    this.runtimeArgs = runtimeArgs;
  }

  protected <T extends PluginContext & DatasetContext> AbstractBatchContext(T context,
                                                                            Metrics metrics,
                                                                            LookupProvider lookup,
                                                                            String stageName,
                                                                            long logicalStartTime,
                                                                            Map<String, String> runtimeArgs) {
    super(context, metrics, lookup, stageName);
    this.datasetContext = context;
    this.logicalStartTime = logicalStartTime;
    this.runtimeArgs = runtimeArgs;
  }


  @Override
  public long getLogicalStartTime() {
    return logicalStartTime;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return Collections.unmodifiableMap(runtimeArgs);
  }

  @Override
  public void setRuntimeArgument(String key, String value, boolean overwrite) {
    if (overwrite || !runtimeArgs.containsKey(key)) {
      runtimeArgs.put(key, value);
    }
  }

  @Override
  public <T extends Dataset> T getDataset(final String name) throws DatasetInstantiationException {
    return LogContext.runWithoutLoggingUnchecked(new Callable<T>() {
      @Override
      public T call() throws Exception {
        return datasetContext.getDataset(name);
      }
    });
  }

  @Override
  public <T extends Dataset> T getDataset(final String name,
                                          final Map<String, String> arguments) throws DatasetInstantiationException {
    return LogContext.runWithoutLoggingUnchecked(new Callable<T>() {
      @Override
      public T call() throws Exception {
        return datasetContext.getDataset(name, arguments);
      }
    });
  }

  @Override
  public void releaseDataset(final Dataset dataset) {
    LogContext.runWithoutLoggingUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        datasetContext.releaseDataset(dataset);
        return null;
      }
    });
  }

  @Override
  public void discardDataset(final Dataset dataset) {
    LogContext.runWithoutLoggingUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        datasetContext.discardDataset(dataset);
        return null;
      }
    });
  }
}
