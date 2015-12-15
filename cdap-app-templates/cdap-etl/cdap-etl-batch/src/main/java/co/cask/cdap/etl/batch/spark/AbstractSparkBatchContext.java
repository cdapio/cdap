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

package co.cask.cdap.etl.batch.spark;

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.common.AbstractTransformContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract implementation of {@link BatchContext} using {@link SparkContext}.
 */
public abstract class AbstractSparkBatchContext extends AbstractTransformContext implements BatchContext {

  private final SparkContext sparkContext;
  private final Map<String, String> runtimeArguments;

  public AbstractSparkBatchContext(SparkContext sparkContext, LookupProvider lookupProvider, String stageId) {
    super(sparkContext.getPluginContext(), sparkContext.getMetrics(), lookupProvider, stageId);
    this.sparkContext = sparkContext;
    this.runtimeArguments = new HashMap<>(sparkContext.getRuntimeArguments());
  }

  @Override
  public long getLogicalStartTime() {
    return sparkContext.getLogicalStartTime();
  }

  @Override
  public <T> T getHadoopJob() {
    throw new UnsupportedOperationException("Hadoop Job is not available in Spark");
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    return sparkContext.getDataset(name);
  }

  @Override
  public <T extends Dataset> T getDataset(String name,
                                          Map<String, String> arguments) throws DatasetInstantiationException {
    return sparkContext.getDataset(name, arguments);
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
