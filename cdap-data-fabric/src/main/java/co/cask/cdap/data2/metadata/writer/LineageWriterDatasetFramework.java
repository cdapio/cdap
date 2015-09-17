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

package co.cask.cdap.data2.metadata.writer;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.service.BusinessMetadataStore;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link DatasetFramework} that also records lineage (program-dataset access) records.
 */
public class LineageWriterDatasetFramework implements DatasetFramework, ProgramContextAware {
  private final DatasetFramework delegate;
  private final LineageWriter lineageWriter;
  private final ProgramContext programContext = new ProgramContext();
  private final BusinessMetadataStore businessMds;

  @Inject
  LineageWriterDatasetFramework(@Named(DataSetsModules.BASIC_DATASET_FRAMEWORK) DatasetFramework datasetFramework,
                                LineageWriter lineageWriter, BusinessMetadataStore businessMds) {
    this.delegate = datasetFramework;
    this.lineageWriter = lineageWriter;
    this.businessMds = businessMds;
  }

  @Override
  public void initContext(Id.Run run) {
    programContext.initContext(run);
  }

  @Override
  public void initContext(Id.Run run, Id.NamespacedId componentId) {
    programContext.initContext(run, componentId);
  }

  @Override
  public void addModule(Id.DatasetModule moduleId, DatasetModule module) throws DatasetManagementException {
    delegate.addModule(moduleId, module);
  }

  @Override
  public void deleteModule(Id.DatasetModule moduleId) throws DatasetManagementException {
    delegate.deleteModule(moduleId);
  }

  @Override
  public void deleteAllModules(Id.Namespace namespaceId) throws DatasetManagementException {
    delegate.deleteAllModules(namespaceId);
  }

  @Override
  public void addInstance(String datasetTypeName, Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {
    delegate.addInstance(datasetTypeName, datasetInstanceId, props);
  }

  @Override
  public void updateInstance(Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {
    delegate.updateInstance(datasetInstanceId, props);
  }

  @Override
  public Collection<DatasetSpecificationSummary> getInstances(Id.Namespace namespaceId)
    throws DatasetManagementException {
    return delegate.getInstances(namespaceId);
  }

  @Override
  @Nullable
  public DatasetSpecification getDatasetSpec(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    return delegate.getDatasetSpec(datasetInstanceId);
  }

  @Override
  public boolean hasInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    return delegate.hasInstance(datasetInstanceId);
  }

  @Override
  public boolean hasSystemType(String typeName) throws DatasetManagementException {
    return delegate.hasSystemType(typeName);
  }

  @Override
  @VisibleForTesting
  public boolean hasType(Id.DatasetType datasetTypeId) throws DatasetManagementException {
    return delegate.hasType(datasetTypeId);
  }

  @Override
  public void deleteInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException, IOException {
    // Remove metadata for the dataset (TODO: https://issues.cask.co/browse/CDAP-3670)
    businessMds.removeMetadata(datasetInstanceId);
    delegate.deleteInstance(datasetInstanceId);
  }

  @Override
  public void deleteAllInstances(Id.Namespace namespaceId) throws DatasetManagementException, IOException {
    Collection<DatasetSpecificationSummary> datasets = this.getInstances(namespaceId);
    for (DatasetSpecificationSummary dataset : datasets) {
      String dsName = dataset.getName();
      Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespaceId, dsName);
      // Remove metadata for the dataset (TODO: https://issues.cask.co/browse/CDAP-3670)
      businessMds.removeMetadata(datasetInstanceId);
    }
    delegate.deleteAllInstances(namespaceId);
  }

  @Override
  @Nullable
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId, @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    return delegate.getAdmin(datasetInstanceId, classLoader);
  }

  @Override
  @Nullable
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId, @Nullable ClassLoader classLoader,
                                             DatasetClassLoaderProvider classLoaderProvider)
    throws DatasetManagementException, IOException {
    return delegate.getAdmin(datasetInstanceId, classLoader, classLoaderProvider);
  }

  @Override
  @Nullable
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments, @Nullable ClassLoader classLoader,
                                          @Nullable Iterable<? extends Id> owners)
    throws DatasetManagementException, IOException {
    T dataset = delegate.getDataset(datasetInstanceId, arguments, classLoader, owners);
    writeLineage(datasetInstanceId, dataset);
    return dataset;
  }

  @Override
  @Nullable
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments, @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    T dataset = delegate.getDataset(datasetInstanceId, arguments, classLoader);
    writeLineage(datasetInstanceId, dataset);
    return dataset;
  }

  @Override
  @Nullable
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments, @Nullable ClassLoader classLoader,
                                          DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable Iterable<? extends Id> owners)
    throws DatasetManagementException, IOException {
    T dataset = delegate.getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider, owners);
    writeLineage(datasetInstanceId, dataset);
    return dataset;
  }

  @Override
  public void createNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    delegate.createNamespace(namespaceId);
  }

  @Override
  public void deleteNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    delegate.deleteNamespace(namespaceId);
  }

  private <T extends Dataset> void writeLineage(Id.DatasetInstance datasetInstanceId, T dataset) {
    if (dataset != null && programContext.getRun() != null) {
      lineageWriter.addAccess(programContext.getRun(), datasetInstanceId, AccessType.UNKNOWN,
                              programContext.getComponentId());
    }
  }
}
