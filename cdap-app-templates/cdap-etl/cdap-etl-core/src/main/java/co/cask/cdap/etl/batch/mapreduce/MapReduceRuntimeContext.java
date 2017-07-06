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
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.common.AbstractTransformContext;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.common.plugin.Caller;
import co.cask.cdap.etl.common.plugin.NoStageLoggingCaller;
import co.cask.cdap.etl.spec.StageSpec;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Batch runtime context that delegates most operations to MapReduceTaskContext. It also extends
 * {@link AbstractTransformContext} in order to provide plugin isolation between pipeline plugins. This means sources,
 * transforms, and sinks don't need to worry that plugins they use conflict with plugins other sources, transforms,
 * or sinks use.
 */
public class MapReduceRuntimeContext extends AbstractTransformContext
  implements BatchRuntimeContext, BatchJoinerRuntimeContext {
  private final MapReduceTaskContext context;
  private final Map<String, String> runtimeArgs;
  private final Caller caller;

  public MapReduceRuntimeContext(MapReduceTaskContext context, Metrics metrics,
                                 LookupProvider lookup, Map<String, String> runtimeArgs, StageSpec stageSpec) {
    super(context, context, metrics, lookup, stageSpec, new BasicArguments(context.getWorkflowToken(), runtimeArgs));
    this.context = context;
    this.runtimeArgs = ImmutableMap.copyOf(runtimeArgs);
    this.caller = NoStageLoggingCaller.wrap(Caller.DEFAULT);
  }

  @Override
  public long getLogicalStartTime() {
    return caller.callUnchecked(new Callable<Long>() {
      @Override
      public Long call() {
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
    return caller.callUnchecked(new Callable<T>() {
      @Override
      public T call() throws DatasetInstantiationException {
        return context.getDataset(name);
      }
    });
  }

  @Override
  public <T extends Dataset> T getDataset(final String namespace, final String name)
    throws DatasetInstantiationException {
    return caller.callUnchecked(new Callable<T>() {
      @Override
      public T call() throws DatasetInstantiationException {
        return context.getDataset(namespace, name);
      }
    });
  }

  @Override
  public <T extends Dataset> T getDataset(final String name,
                                          final Map<String, String> arguments) throws DatasetInstantiationException {
    return caller.callUnchecked(new Callable<T>() {
      @Override
      public T call() throws DatasetInstantiationException {
        return context.getDataset(name, arguments);
      }
    });
  }

  @Override
  public <T extends Dataset> T getDataset(final String namespace, final String name,
                                          final Map<String, String> arguments) throws DatasetInstantiationException {
    return caller.callUnchecked(new Callable<T>() {
      @Override
      public T call() throws DatasetInstantiationException {
        return context.getDataset(namespace, name, arguments);
      }
    });
  }

  @Override
  public void releaseDataset(final Dataset dataset) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        context.releaseDataset(dataset);
        return null;
      }
    });
  }

  @Override
  public void discardDataset(final Dataset dataset) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        context.discardDataset(dataset);
        return null;
      }
    });
  }
}
