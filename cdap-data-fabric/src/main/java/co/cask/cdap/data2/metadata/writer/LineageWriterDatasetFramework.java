/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.audit.AuditPublisher;
import co.cask.cdap.data2.audit.AuditPublishers;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.ForwardingDatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.registry.RuntimeUsageRegistry;
import co.cask.cdap.proto.Id;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link DatasetFramework} that also records lineage (program-dataset access) records.
 */
public class LineageWriterDatasetFramework extends ForwardingDatasetFramework implements ProgramContextAware {

  private static final Logger LOG = LoggerFactory.getLogger(LineageWriterDatasetFramework.class);

  private final RuntimeUsageRegistry runtimeUsageRegistry;
  private final LineageWriter lineageWriter;
  private final ProgramContext programContext = new ProgramContext();

  private AuditPublisher auditPublisher;

  @Inject
  public LineageWriterDatasetFramework(@Named(DataSetsModules.BASIC_DATASET_FRAMEWORK)
                                         DatasetFramework datasetFramework,
                                       LineageWriter lineageWriter,
                                       RuntimeUsageRegistry runtimeUsageRegistry) {
    super(datasetFramework);
    this.lineageWriter = lineageWriter;
    this.runtimeUsageRegistry = runtimeUsageRegistry;
  }

  @SuppressWarnings("unused")
  @Inject(optional = true)
  public void setAuditPublisher(AuditPublisher auditPublisher) {
    this.auditPublisher = auditPublisher;
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
  public void addInstance(String datasetTypeName, Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {
    super.addInstance(datasetTypeName, datasetInstanceId, props);

  }

  @Override
  public void updateInstance(Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {
    super.updateInstance(datasetInstanceId, props);
  }

  @Override
  public void deleteInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException, IOException {
    delegate.deleteInstance(datasetInstanceId);
  }

  @Override
  public void deleteAllInstances(Id.Namespace namespaceId) throws DatasetManagementException, IOException {
    delegate.deleteAllInstances(namespaceId);
  }

  @Override
  @Nullable
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader,
                                          @Nullable Iterable<? extends Id> owners)
    throws DatasetManagementException, IOException {
    T dataset = super.getDataset(datasetInstanceId, arguments, classLoader, owners);
    writeLineage(datasetInstanceId, dataset);
    writeUsage(datasetInstanceId, owners);
    return dataset;
  }

  @Override
  @Nullable
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    T dataset = super.getDataset(datasetInstanceId, arguments, classLoader);
    writeLineage(datasetInstanceId, dataset);
    return dataset;
  }

  @Override
  @Nullable
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader,
                                          DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable Iterable<? extends Id> owners)
    throws DatasetManagementException, IOException {
    T dataset = super.getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider, owners);
    writeLineage(datasetInstanceId, dataset);
    writeUsage(datasetInstanceId, owners);
    return dataset;
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader,
                                          DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable Iterable<? extends Id> owners,
                                          AccessType accessType)
    throws DatasetManagementException, IOException {
    T dataset = super.getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider, owners, accessType);
    writeLineage(datasetInstanceId, dataset, accessType);
    writeUsage(datasetInstanceId, owners);
    return dataset;
  }

  private void writeUsage(Id.DatasetInstance datasetInstanceId, @Nullable Iterable<? extends Id> owners) {
    if (null == owners) {
      return;
    }
    try {
      runtimeUsageRegistry.registerAll(owners, datasetInstanceId);
    } catch (Exception e) {
      LOG.warn("Failed to register usage of {} -> {}", owners, datasetInstanceId, e);
    }
  }

  @Override
  public void writeLineage(Id.DatasetInstance datasetInstanceId, AccessType accessType) {
    super.writeLineage(datasetInstanceId, accessType);
    doWriteLineage(datasetInstanceId, accessType);
  }

  private <T extends Dataset> void writeLineage(Id.DatasetInstance datasetInstanceId, @Nullable T dataset) {
    writeLineage(datasetInstanceId, dataset, AccessType.UNKNOWN);
  }

  private <T extends Dataset> void writeLineage(Id.DatasetInstance datasetInstanceId, @Nullable T dataset,
                                                AccessType accessType) {
    if (dataset != null) {
      doWriteLineage(datasetInstanceId, accessType);
    }
  }

  private void doWriteLineage(Id.DatasetInstance datasetInstanceId, AccessType accessType) {
    if (programContext.getRun() != null) {
      lineageWriter.addAccess(programContext.getRun(), datasetInstanceId, accessType,
                              programContext.getComponentId());
      AuditPublishers.publishAccess(auditPublisher, datasetInstanceId, accessType, programContext.getRun());
    }
  }

}
