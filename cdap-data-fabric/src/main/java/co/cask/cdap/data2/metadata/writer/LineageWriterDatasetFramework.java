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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.audit.AuditPublisher;
import co.cask.cdap.data2.audit.AuditPublishers;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.ForwardingDatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.system.AbstractSystemMetadataWriter;
import co.cask.cdap.data2.metadata.system.DatasetSystemMetadataWriter;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link DatasetFramework} that also records lineage (program-dataset access) records.
 */
public class LineageWriterDatasetFramework extends ForwardingDatasetFramework implements ProgramContextAware {
  private final LineageWriter lineageWriter;
  private final ProgramContext programContext = new ProgramContext();
  private final MetadataStore metadataStore;
  private final SystemDatasetInstantiatorFactory dsInstantiatorFactory;

  private AuditPublisher auditPublisher;

  @Inject
  public LineageWriterDatasetFramework(
    @Named(DataSetsModules.BASIC_DATASET_FRAMEWORK) DatasetFramework datasetFramework,
    LineageWriter lineageWriter, MetadataStore metadataStore,
    LocationFactory locationFactory, CConfiguration cConf) {
    super(datasetFramework);
    this.lineageWriter = lineageWriter;
    this.metadataStore = metadataStore;
    this.dsInstantiatorFactory = new SystemDatasetInstantiatorFactory(locationFactory, datasetFramework, cConf);
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
    // add system metadata for user datasets only
    if (!isUserDataset(datasetInstanceId)) {
      return;
    }
    AbstractSystemMetadataWriter systemMetadataWriter =
      new DatasetSystemMetadataWriter(metadataStore, dsInstantiatorFactory, datasetInstanceId, props, datasetTypeName);
    systemMetadataWriter.write();
  }

  //TODO: CDAP-4627 - Figure out a better way to identify system datasets in user namespaces
  private boolean isUserDataset(Id.DatasetInstance datasetInstanceId) {
    return !Id.Namespace.SYSTEM.equals(datasetInstanceId.getNamespace()) &&
      !"system.queue.config".equals(datasetInstanceId.getId()) &&
      !datasetInstanceId.getId().startsWith("system.sharded.queue") &&
      !datasetInstanceId.getId().startsWith("system.queue") &&
      !datasetInstanceId.getId().startsWith("system.stream");
  }

  @Override
  public void updateInstance(Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {
    super.updateInstance(datasetInstanceId, props);
    // add system metadata for user datasets only
    if (!isUserDataset(datasetInstanceId)) {
      return;
    }
    AbstractSystemMetadataWriter systemMetadataWriter =
      new DatasetSystemMetadataWriter(metadataStore, dsInstantiatorFactory, datasetInstanceId, props, null);
    systemMetadataWriter.write();
  }

  @Override
  public void deleteInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException, IOException {
    // Remove metadata for the dataset (TODO: https://issues.cask.co/browse/CDAP-3670)
    metadataStore.removeMetadata(datasetInstanceId);
    delegate.deleteInstance(datasetInstanceId);
  }

  @Override
  public void deleteAllInstances(Id.Namespace namespaceId) throws DatasetManagementException, IOException {
    Collection<DatasetSpecificationSummary> datasets = this.getInstances(namespaceId);
    for (DatasetSpecificationSummary dataset : datasets) {
      String dsName = dataset.getName();
      Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespaceId, dsName);
      // Remove metadata for the dataset (TODO: https://issues.cask.co/browse/CDAP-3670)
      metadataStore.removeMetadata(datasetInstanceId);
    }
    delegate.deleteAllInstances(namespaceId);
  }

  @Override
  @Nullable
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments, @Nullable ClassLoader classLoader,
                                          @Nullable Iterable<? extends Id> owners)
    throws DatasetManagementException, IOException {
    T dataset = super.getDataset(datasetInstanceId, arguments, classLoader, owners);
    writeLineage(datasetInstanceId, dataset);
    return dataset;
  }

  @Override
  @Nullable
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId,
                                          @Nullable Map<String, String> arguments, @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    T dataset = super.getDataset(datasetInstanceId, arguments, classLoader);
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
    T dataset = super.getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider, owners);
    writeLineage(datasetInstanceId, dataset);
    return dataset;
  }

  private <T extends Dataset> void writeLineage(Id.DatasetInstance datasetInstanceId, T dataset) {
    if (dataset != null && programContext.getRun() != null) {
      lineageWriter.addAccess(programContext.getRun(), datasetInstanceId, AccessType.UNKNOWN,
                              programContext.getComponentId());
      AuditPublishers.publishAccess(auditPublisher, datasetInstanceId, AccessType.UNKNOWN, programContext.getRun());
    }
  }
}
