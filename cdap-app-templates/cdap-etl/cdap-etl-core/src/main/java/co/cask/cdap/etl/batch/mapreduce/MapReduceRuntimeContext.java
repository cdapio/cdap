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

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.common.AbstractTransformContext;
import co.cask.cdap.etl.log.LogContext;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Batch runtime context that delegates most operations to MapReduceTaskContext. It also extends
 * {@link AbstractTransformContext} in order to provide plugin isolation between pipeline plugins. This means sources,
 * transforms, and sinks don't need to worry that plugins they use conflict with plugins other sources, transforms,
 * or sinks use.
 */
public class MapReduceRuntimeContext extends AbstractTransformContext implements BatchRuntimeContext {
  private final MapReduceTaskContext context;
  private final Map<String, String> runtimeArgs;

  public MapReduceRuntimeContext(MapReduceTaskContext context, Metrics metrics,
                                 LookupProvider lookup, String stageName,
                                 Map<String, String> runtimeArgs) {
    super(context, metrics, lookup, stageName);
    this.context = context;
    this.runtimeArgs = ImmutableMap.copyOf(runtimeArgs);
  }

  @Override
  public long getLogicalStartTime() {
    return LogContext.runWithoutLoggingUnchecked(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return context.getLogicalStartTime();
      }
    });
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArgs;
  }

  @Override
  public <T> T getHadoopJob() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public <T extends Dataset> T getDataset(final String name) throws DatasetInstantiationException {
    return LogContext.runWithoutLoggingUnchecked(new Callable<T>() {
      @Override
      public T call() throws DatasetInstantiationException {
        return context.getDataset(name);
      }
    });
  }

  @Override
  public <T extends Dataset> T getDataset(final String name,
                                          final Map<String, String> arguments) throws DatasetInstantiationException {
    return LogContext.runWithoutLoggingUnchecked(new Callable<T>() {
      @Override
      public T call() throws DatasetInstantiationException {
        return context.getDataset(name, arguments);
      }
    });
  }

  @Override
  public void releaseDataset(final Dataset dataset) {
    LogContext.runWithoutLoggingUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        context.releaseDataset(dataset);
        return null;
      }
    });
  }

  @Override
  public void discardDataset(final Dataset dataset) {
    LogContext.runWithoutLoggingUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        context.discardDataset(dataset);
        return null;
      }
    });
  }
}
