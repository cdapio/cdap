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
package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.ForwardingProgramContextAwareDatasetFramework;
import co.cask.cdap.data2.metadata.writer.ProgramContextAware;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of the {@link DatasetFramework} which forwards all calls to the underlying
 * dataset framework. Before forwarding the calls, local dataset names are mapped to the names
 * assigned to them for the current run of the {@link Workflow}.
 */
public class WorkflowDatasetFramework extends ForwardingProgramContextAwareDatasetFramework {

  private final Map<String, String> datasetNameMapping;

  public WorkflowDatasetFramework(DatasetFramework delegate, Map<String, String> datasetNameMapping) {
    super(delegate);
    this.datasetNameMapping = datasetNameMapping;
  }

  /**
   * Return the dataset name mapping.
   */
  public Map<String, String> getDatasetNameMapping() {
    return datasetNameMapping;
  }

  @Override
  public void addInstance(String datasetTypeName, Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws IOException, DatasetManagementException {
    super.addInstance(datasetTypeName, getMappedDatasetInstance(datasetInstanceId), props);
  }

  @Override
  public void updateInstance(Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {
    super.updateInstance(getMappedDatasetInstance(datasetInstanceId), props);
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    return super.getDatasetSpec(getMappedDatasetInstance(datasetInstanceId));
  }

  @Override
  public boolean hasInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    return super.hasInstance(getMappedDatasetInstance(datasetInstanceId));
  }

  @Override
  public void deleteInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException, IOException {
    super.deleteInstance(getMappedDatasetInstance(datasetInstanceId));
  }

  @Nullable
  @Override
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId, @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    return super.getAdmin(getMappedDatasetInstance(datasetInstanceId), classLoader);
  }

  @Nullable
  @Override
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId, @Nullable ClassLoader classLoader,
                                             DatasetClassLoaderProvider classLoaderProvider)
    throws DatasetManagementException, IOException {
    return super.getAdmin(getMappedDatasetInstance(datasetInstanceId), classLoader, classLoaderProvider);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId, @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader, @Nullable Iterable<? extends Id> owners)
    throws DatasetManagementException, IOException {
    return super.getDataset(getMappedDatasetInstance(datasetInstanceId), arguments, classLoader, owners);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId, @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    return super.getDataset(getMappedDatasetInstance(datasetInstanceId), arguments, classLoader);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId, @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader,
                                          DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable Iterable<? extends Id> owners)
    throws DatasetManagementException, IOException {
    return super.getDataset(getMappedDatasetInstance(datasetInstanceId), arguments, classLoader,
                               classLoaderProvider, owners);
  }

  private Id.DatasetInstance getMappedDatasetInstance(Id.DatasetInstance datasetInstanceId) {
    if (datasetNameMapping.containsKey(datasetInstanceId.getId())) {
      return Id.DatasetInstance.from(datasetInstanceId.getNamespaceId(),
                                     datasetNameMapping.get(datasetInstanceId.getId()));
    }
    return datasetInstanceId;
  }
}
