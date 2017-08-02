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

package co.cask.cdap.etl.spark.batch;

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.common.AbstractTransformContext;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.spark.NoLookupProvider;
import co.cask.cdap.etl.spec.StageSpec;

import java.util.Map;

/**
 * Default implementation of {@link BatchRuntimeContext} for spark contexts.
 */
public class SparkBatchRuntimeContext extends AbstractTransformContext
  implements BatchRuntimeContext, BatchJoinerRuntimeContext {

  public SparkBatchRuntimeContext(PipelineRuntime pipelineRuntime, StageSpec stageSpec) {
    super(pipelineRuntime, stageSpec, NoLookupProvider.INSTANCE);
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return arguments.asMap();
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name) throws DatasetInstantiationException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public <T extends Dataset> T getDataset(String name,
                                          Map<String, String> arguments) throws DatasetInstantiationException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name,
                                          Map<String, String> arguments) throws DatasetInstantiationException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void releaseDataset(Dataset dataset) {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void discardDataset(Dataset dataset) {
    throw new UnsupportedOperationException("Not supported");
  }
}
