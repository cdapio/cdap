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
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.common.AbstractTransformContext;
import co.cask.cdap.etl.log.LogContext;
import com.google.common.base.Throwables;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Abstract implementation of {@link BatchContext} using {@link MapReduceContext}.
 */
public abstract class MapReduceBatchContext extends AbstractTransformContext implements BatchContext {

  protected final MapReduceContext mrContext;
  protected final LookupProvider lookup;
  private final Map<String, String> runtimeArguments;

  public MapReduceBatchContext(MapReduceContext context, Metrics metrics,
                               LookupProvider lookup, String stageName, Map<String, String> runtimeArguments) {
    super(context, metrics, lookup, stageName);
    this.mrContext = context;
    this.lookup = lookup;
    this.runtimeArguments = new HashMap<>(runtimeArguments);
  }

  @Override
  public long getLogicalStartTime() {
    return LogContext.runWithoutLoggingUnchecked(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return mrContext.getLogicalStartTime();
      }
    });
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

  @Override
  public <T extends Dataset> T getDataset(final String name) throws DatasetInstantiationException {
    try {
      return LogContext.runWithoutLogging(new Callable<T>() {
        @Override
        public T call() throws Exception {
          return mrContext.getDataset(name);
        }
      });
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, DatasetInstantiationException.class);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <T extends Dataset> T getDataset(final String name, final Map<String, String> arguments)
    throws DatasetInstantiationException {
    try {
      return LogContext.runWithoutLogging(new Callable<T>() {
        @Override
        public T call() throws Exception {
          return mrContext.getDataset(name, arguments);
        }
      });
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, DatasetInstantiationException.class);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return Collections.unmodifiableMap(runtimeArguments);
  }

  @Override
  public void setRuntimeArgument(String key, String value, boolean overwrite) {
    if (overwrite || !runtimeArguments.containsKey(key)) {
      runtimeArguments.put(key, value);
    }
  }
}
