/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.etl.api.batch.BatchContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.common.AbstractTransformContext;
import io.cdap.cdap.etl.common.DatasetContextLookupProvider;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.plugin.Caller;
import io.cdap.cdap.etl.common.plugin.NoStageLoggingCaller;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Base Batch Context.
 */
public abstract class AbstractBatchContext extends AbstractTransformContext implements BatchContext {
  private static final Caller CALLER = NoStageLoggingCaller.wrap(Caller.DEFAULT);
  private final DatasetContext datasetContext;
  protected final Admin admin;
  private final List<FieldOperation> fieldOperations;

  protected AbstractBatchContext(PipelineRuntime pipelineRuntime, StageSpec stageSpec,
                                 DatasetContext datasetContext, Admin admin) {
    super(pipelineRuntime, stageSpec, new DatasetContextLookupProvider(datasetContext));
    this.datasetContext = datasetContext;
    this.admin = admin;
    this.fieldOperations = new ArrayList<>();
  }

  @Override
  public void createDataset(String datasetName, String typeName, DatasetProperties properties)
    throws DatasetManagementException, AccessException {
    admin.createDataset(datasetName, typeName, properties);
  }

  @Override
  public boolean datasetExists(String datasetName) throws DatasetManagementException, AccessException {
    return admin.datasetExists(datasetName);
  }

  @Override
  public <T extends Dataset> T getDataset(final String name) throws DatasetInstantiationException {
    return CALLER.callUnchecked(() -> datasetContext.getDataset(name));
  }

  @Override
  public <T extends Dataset> T getDataset(final String namespace, final String name)
    throws DatasetInstantiationException {
    return CALLER.callUnchecked(() -> datasetContext.getDataset(namespace, name));
  }

  @Override
  public <T extends Dataset> T getDataset(final String name,
                                          final Map<String, String> arguments) throws DatasetInstantiationException {
    return CALLER.callUnchecked(() -> datasetContext.getDataset(name, arguments));
  }

  @Override
  public <T extends Dataset> T getDataset(final String namespace, final String name,
                                          final Map<String, String> arguments) throws DatasetInstantiationException {
    return CALLER.callUnchecked(() -> datasetContext.getDataset(namespace, name, arguments));
  }

  @Override
  public void releaseDataset(final Dataset dataset) {
    CALLER.callUnchecked(() -> {
      datasetContext.releaseDataset(dataset);
      return null;
    });
  }

  @Override
  public void discardDataset(final Dataset dataset) {
    CALLER.callUnchecked(() -> {
      datasetContext.discardDataset(dataset);
      return null;
    });
  }

  @Override
  public void record(List<FieldOperation> fieldOperations) {
    this.fieldOperations.addAll(fieldOperations);
  }

  public List<FieldOperation> getFieldOperations() {
    return Collections.unmodifiableList(fieldOperations);
  }
}
