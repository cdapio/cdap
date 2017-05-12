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

package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.ForwardingProgramContextAwareDatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import com.google.common.base.Function;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Implementation of the {@link DatasetFramework} that supports renaming datasets before forwarding calls to
 * the underlying dataset framework.
 */
public class NameMappedDatasetFramework extends ForwardingProgramContextAwareDatasetFramework {

  private final Map<String, String> datasetNameMapping;

  /**
   * Creates a new instance based on the given {@link WorkflowProgramInfo}.
   */
  public static NameMappedDatasetFramework createFromWorkflowProgramInfo(DatasetFramework datasetFramework,
                                                                         WorkflowProgramInfo info,
                                                                         ApplicationSpecification appSpec) {
    Set<String> localDatasets = appSpec.getWorkflows().get(info.getName()).getLocalDatasetSpecs().keySet();
    return new NameMappedDatasetFramework(datasetFramework, localDatasets, info.getRunId().getId());
  }

  /**
   * Creates a new instance which maps the given set of dataset names by appending ("." + nameSuffix).
   */
  public NameMappedDatasetFramework(DatasetFramework delegate, Set<String> datasets, final String nameSuffix) {
    this(delegate, datasets, new Function<String, String>() {
      @Override
      public String apply(String dataset) {
        return dataset + "." + nameSuffix;
      }
    });
  }

  /**
   * Creates a new instance which maps the given set of dataset names by applying the transform function.
   */
  public NameMappedDatasetFramework(DatasetFramework delegate, Set<String> datasets,
                                    Function<String, String> nameTransformer) {
    super(delegate);
    Map<String, String> nameMapping = new HashMap<>();
    for (String dataset : datasets) {
      nameMapping.put(dataset, nameTransformer.apply(dataset));
    }
    this.datasetNameMapping = Collections.unmodifiableMap(nameMapping);
  }

  /**
   * Return the dataset name mapping.
   */
  public Map<String, String> getDatasetNameMapping() {
    return datasetNameMapping;
  }

  @Override
  public void addInstance(String datasetTypeName, DatasetId datasetInstanceId, DatasetProperties props)
    throws IOException, DatasetManagementException {
    addInstance(datasetTypeName, datasetInstanceId, props, null);
  }

  @Override
  public void addInstance(String datasetTypeName, DatasetId datasetInstanceId, DatasetProperties props,
                          @Nullable KerberosPrincipalId ownerPrincipal)
    throws IOException, DatasetManagementException {
    super.addInstance(datasetTypeName, getMappedDatasetInstance(datasetInstanceId), props, ownerPrincipal);
  }

  @Override
  public void updateInstance(DatasetId datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {
    super.updateInstance(getMappedDatasetInstance(datasetInstanceId), props);
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(DatasetId datasetInstanceId) throws DatasetManagementException {
    return super.getDatasetSpec(getMappedDatasetInstance(datasetInstanceId));
  }

  @Override
  public boolean hasInstance(DatasetId datasetInstanceId) throws DatasetManagementException {
    return super.hasInstance(getMappedDatasetInstance(datasetInstanceId));
  }

  @Override
  public void deleteInstance(DatasetId datasetInstanceId) throws DatasetManagementException, IOException {
    super.deleteInstance(getMappedDatasetInstance(datasetInstanceId));
  }

  @Nullable
  @Override
  public <T extends DatasetAdmin> T getAdmin(DatasetId datasetInstanceId, @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    return super.getAdmin(getMappedDatasetInstance(datasetInstanceId), classLoader);
  }

  @Nullable
  @Override
  public <T extends DatasetAdmin> T getAdmin(DatasetId datasetInstanceId, @Nullable ClassLoader classLoader,
                                             DatasetClassLoaderProvider classLoaderProvider)
    throws DatasetManagementException, IOException {
    return super.getAdmin(getMappedDatasetInstance(datasetInstanceId), classLoader, classLoaderProvider);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(DatasetId datasetInstanceId, Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    return super.getDataset(getMappedDatasetInstance(datasetInstanceId), arguments, classLoader);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(DatasetId datasetInstanceId, Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader,
                                          DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable Iterable<? extends EntityId> owners, AccessType accessType)
    throws DatasetManagementException, IOException {
    return super.getDataset(getMappedDatasetInstance(datasetInstanceId),
                            arguments, classLoader, classLoaderProvider, owners, accessType);
  }

  @Override
  public void writeLineage(DatasetId datasetInstanceId, AccessType accessType) {
    super.writeLineage(getMappedDatasetInstance(datasetInstanceId), accessType);
  }

  private DatasetId getMappedDatasetInstance(DatasetId datasetInstanceId) {
    if (datasetNameMapping.containsKey(datasetInstanceId.getEntityName())) {
      return datasetInstanceId.getParent().dataset(datasetNameMapping.get(datasetInstanceId.getEntityName()));
    }
    return datasetInstanceId;
  }
}
