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

package co.cask.cdap.data2.dataset2.preview;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.datafabric.dataset.type.ConstantClassLoaderProvider;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetRuntimeContext;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.NoOpAuthorizer;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Dataset framework that delegates either to local or shared (actual) dataset framework.
 */
public class PreviewDatasetFramework implements DatasetFramework {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewDatasetFramework.class);
  private static final DatasetAdmin NOOP_DATASET_ADMIN = new DatasetAdmin() {
    @Override
    public boolean exists() throws IOException {
      return true;
    }

    @Override
    public void create() throws IOException {
    }

    @Override
    public void drop() throws IOException {
    }

    @Override
    public void truncate() throws IOException {
    }

    @Override
    public void upgrade() throws IOException {
    }

    @Override
    public void close() throws IOException {
    }
  };
  private static final AuthorizationEnforcer NOOP_ENFORCER = new NoOpAuthorizer();
  private static final DefaultDatasetRuntimeContext.DatasetAccessRecorder NOOP_DATASET_ACCESS_RECORDER =
    new DefaultDatasetRuntimeContext.DatasetAccessRecorder() {
      @Override
      public void recordLineage(AccessType accessType) {
        // no-op
      }

      @Override
      public void emitAudit(AccessType accessType) {
        // no-op
      }
    };

  private final DatasetFramework localDatasetFramework;
  private final DatasetFramework actualDatasetFramework;
  private final AuthenticationContext authenticationContext;
  private final AuthorizationEnforcer authorizationEnforcer;

  /**
   * Create instance of the {@link PreviewDatasetFramework}.
   *
   * @param local the dataset framework instance in the preview space
   * @param actual the dataset framework instance in the real space
   */
  public PreviewDatasetFramework(DatasetFramework local, DatasetFramework actual,
                                 AuthenticationContext authenticationContext,
                                 AuthorizationEnforcer authorizationEnforcer) {
    this.localDatasetFramework = local;
    this.actualDatasetFramework = actual;
    this.authenticationContext = authenticationContext;
    this.authorizationEnforcer = authorizationEnforcer;
  }

  @Override
  public void addModule(DatasetModuleId moduleId, DatasetModule module) throws DatasetManagementException {
    localDatasetFramework.addModule(moduleId, module);
  }

  @Override
  public void addModule(DatasetModuleId moduleId, DatasetModule module, Location jarLocation)
    throws DatasetManagementException {
    // called while deploying the new application from DatasetModuleDeployer stage.
    // any new module should be deployed in the preview space.
    localDatasetFramework.addModule(moduleId, module, jarLocation);
  }

  @Override
  public void deleteModule(DatasetModuleId moduleId) throws DatasetManagementException {
    localDatasetFramework.deleteModule(moduleId);
  }

  @Override
  public void deleteAllModules(NamespaceId namespaceId) throws DatasetManagementException {
    localDatasetFramework.deleteAllModules(namespaceId);
  }

  @Override
  public void addInstance(String datasetTypeName, DatasetId datasetInstanceId,
                          DatasetProperties props) throws DatasetManagementException, IOException {
    localDatasetFramework.addInstance(datasetTypeName, datasetInstanceId, props);
  }

  @Override
  public void addInstance(String datasetTypeName, DatasetId datasetInstanceId,
                          DatasetProperties props,
                          @Nullable KerberosPrincipalId ownerPrincipal) throws DatasetManagementException, IOException {
    if (ownerPrincipal == null) {
      addInstance(datasetTypeName, datasetInstanceId, props);
      return;
    }
    throw new UnsupportedOperationException("Creating dataset instance with owner is not supported in preview, " +
                                              "please try to start the preview without the ownership");
  }

  @Override
  public void updateInstance(DatasetId datasetInstanceId,
                             DatasetProperties props) throws DatasetManagementException, IOException {
    // allow updates to the datasets in preview space only
    if (localDatasetFramework.hasInstance(datasetInstanceId)) {
      localDatasetFramework.updateInstance(datasetInstanceId, props);
    }
  }

  @Override
  public Collection<DatasetSpecificationSummary> getInstances(NamespaceId namespaceId)
    throws DatasetManagementException {
    return localDatasetFramework.getInstances(namespaceId);
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(DatasetId datasetInstanceId) throws DatasetManagementException {
    if (DatasetsUtil.isUserDataset(datasetInstanceId)) {
      DatasetSpecification datasetSpec = actualDatasetFramework.getDatasetSpec(datasetInstanceId);
      return datasetSpec != null ? datasetSpec : localDatasetFramework.getDatasetSpec(datasetInstanceId);
    }
    return localDatasetFramework.getDatasetSpec(datasetInstanceId);
  }

  @Override
  public boolean hasInstance(DatasetId datasetInstanceId) throws DatasetManagementException {
    if (DatasetsUtil.isUserDataset(datasetInstanceId)) {
      return actualDatasetFramework.hasInstance(datasetInstanceId) ||
        localDatasetFramework.hasInstance(datasetInstanceId);
    }
    return localDatasetFramework.hasInstance(datasetInstanceId);
  }

  @Override
  public boolean hasSystemType(String typeName) throws DatasetManagementException {
    return hasType(NamespaceId.SYSTEM.datasetType(typeName));
  }

  @Override
  public boolean hasType(DatasetTypeId datasetTypeId) throws DatasetManagementException {
    return localDatasetFramework.hasType(datasetTypeId);
  }

  @Nullable
  @Override
  public DatasetTypeMeta getTypeInfo(DatasetTypeId datasetTypeId) throws DatasetManagementException {
    return localDatasetFramework.getTypeInfo(datasetTypeId);
  }

  @Override
  public void truncateInstance(DatasetId datasetInstanceId) throws DatasetManagementException, IOException {
    // If dataset exists in the preview space then only truncate it otherwise its a no-op
    if (localDatasetFramework.hasInstance(datasetInstanceId)) {
      localDatasetFramework.truncateInstance(datasetInstanceId);
    }
  }

  @Override
  public void deleteInstance(DatasetId datasetInstanceId) throws DatasetManagementException, IOException {
    // If dataset exists in the preview space then only delete it otherwise its a no-op
    if (localDatasetFramework.hasInstance(datasetInstanceId)) {
      localDatasetFramework.deleteInstance(datasetInstanceId);
    }
  }

  @Override
  public void deleteAllInstances(NamespaceId namespaceId) throws DatasetManagementException, IOException {
    localDatasetFramework.deleteAllInstances(namespaceId);
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T extends DatasetAdmin> T getAdmin(DatasetId datasetInstanceId, @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    // Return the no-op admin for the dataset from the real space
    if (actualDatasetFramework.hasInstance(datasetInstanceId)) {
      return (T) NOOP_DATASET_ADMIN;
    }
    return localDatasetFramework.getAdmin(datasetInstanceId, classLoader);
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T extends DatasetAdmin> T getAdmin(DatasetId datasetInstanceId, @Nullable ClassLoader classLoader,
                                             DatasetClassLoaderProvider classLoaderProvider)
    throws DatasetManagementException, IOException {
    // Return the no-op admin for the dataset from the real space
    if (actualDatasetFramework.hasInstance(datasetInstanceId)) {
      return (T) NOOP_DATASET_ADMIN;
    }
    return localDatasetFramework.getAdmin(datasetInstanceId, classLoader, classLoaderProvider);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(DatasetId datasetInstanceId, @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {

    return getDataset(datasetInstanceId, arguments, classLoader, new ConstantClassLoaderProvider(classLoader), null,
                      AccessType.UNKNOWN);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(final DatasetId datasetInstanceId,
                                          @Nullable final Map<String, String> arguments,
                                          @Nullable final ClassLoader classLoader,
                                          final DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable final Iterable<? extends EntityId> owners,
                                          final AccessType accessType) throws DatasetManagementException, IOException {
    Principal principal = authenticationContext.getPrincipal();

    try {
      AuthorizationEnforcer enforcer;

      final boolean isUserDataset = DatasetsUtil.isUserDataset(datasetInstanceId);
      // only for the datasets from the real space enforce the authorization.
      if (isUserDataset && actualDatasetFramework.hasInstance(datasetInstanceId)) {
        enforcer = authorizationEnforcer;
      } else {
        enforcer = NOOP_ENFORCER;
      }

      return DefaultDatasetRuntimeContext.execute(enforcer, NOOP_DATASET_ACCESS_RECORDER, principal, datasetInstanceId,
                                                  null, new Callable<T>() {
          @Override
          public T call() throws Exception {
            if (isUserDataset && actualDatasetFramework.hasInstance(datasetInstanceId)) {
              return actualDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider,
                                                       owners, accessType);
            }
            return localDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider,
                                                    owners, accessType);
          }
        });
    } catch (IOException | DatasetManagementException e) {
      throw e;
    } catch (Exception e) {
      throw new DatasetManagementException("Failed to create dataset instance: " + datasetInstanceId, e);
    }
  }

  @Override
  public void writeLineage(DatasetId datasetInstanceId, AccessType accessType) {
    // no-op
  }
}
