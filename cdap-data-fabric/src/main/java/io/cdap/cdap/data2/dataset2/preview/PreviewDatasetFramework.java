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

package io.cdap.cdap.data2.dataset2.preview;

import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.data2.datafabric.dataset.DatasetsUtil;
import io.cdap.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DefaultDatasetRuntimeContext;
import io.cdap.cdap.data2.dataset2.ForwardingDatasetFramework;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.security.spi.authorization.NoOpAuthorizer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Dataset framework that delegates either to local or shared (actual) dataset framework.
 */
public class PreviewDatasetFramework extends ForwardingDatasetFramework {

  private static final DatasetAdmin NOOP_DATASET_ADMIN = new DatasetAdmin() {
    @Override
    public boolean exists() {
      return true;
    }

    @Override
    public void create() {
    }

    @Override
    public void drop() {
    }

    @Override
    public void truncate() {
    }

    @Override
    public void upgrade() {
    }

    @Override
    public void close() {
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
    super(local);
    this.actualDatasetFramework = actual;
    this.authenticationContext = authenticationContext;
    this.authorizationEnforcer = authorizationEnforcer;
  }

  @Override
  public void addInstance(String datasetTypeName, DatasetId datasetInstanceId,
                          DatasetProperties props,
                          @Nullable KerberosPrincipalId ownerPrincipal)
    throws DatasetManagementException, IOException, UnauthorizedException {
    if (ownerPrincipal != null) {
      throw new UnsupportedOperationException("Creating dataset instance with owner is not supported in preview, " +
                                                "please try to start the preview without the ownership");
    }
    super.addInstance(datasetTypeName, datasetInstanceId, props, null);
  }

  @Override
  public void updateInstance(DatasetId datasetInstanceId,
                             DatasetProperties props)
    throws DatasetManagementException, IOException, UnauthorizedException {
    // allow updates to the datasets in preview space only
    if (delegate.hasInstance(datasetInstanceId)) {
      delegate.updateInstance(datasetInstanceId, props);
    }
  }


  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(DatasetId datasetInstanceId)
    throws DatasetManagementException, UnauthorizedException {
    if (DatasetsUtil.isUserDataset(datasetInstanceId)) {
      DatasetSpecification datasetSpec = actualDatasetFramework.getDatasetSpec(datasetInstanceId);
      return datasetSpec != null ? datasetSpec : delegate.getDatasetSpec(datasetInstanceId);
    }
    return delegate.getDatasetSpec(datasetInstanceId);
  }

  @Override
  public boolean hasInstance(DatasetId datasetInstanceId)
    throws DatasetManagementException, UnauthorizedException {
    if (DatasetsUtil.isUserDataset(datasetInstanceId)) {
      return actualDatasetFramework.hasInstance(datasetInstanceId) || delegate.hasInstance(datasetInstanceId);
    }
    return delegate.hasInstance(datasetInstanceId);
  }

  @Override
  public void truncateInstance(DatasetId datasetInstanceId)
    throws DatasetManagementException, IOException, UnauthorizedException {
    // If dataset exists in the preview space then only truncate it otherwise its a no-op
    if (delegate.hasInstance(datasetInstanceId)) {
      delegate.truncateInstance(datasetInstanceId);
    }
  }

  @Override
  public void deleteInstance(DatasetId datasetInstanceId)
    throws DatasetManagementException, IOException, UnauthorizedException {
    // If dataset exists in the preview space then only delete it otherwise its a no-op
    if (delegate.hasInstance(datasetInstanceId)) {
      delegate.deleteInstance(datasetInstanceId);
    }
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T extends DatasetAdmin> T getAdmin(DatasetId datasetInstanceId, @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException, UnauthorizedException {
    // Return the no-op admin for the dataset from the real space
    if (actualDatasetFramework.hasInstance(datasetInstanceId)) {
      return (T) NOOP_DATASET_ADMIN;
    }
    return delegate.getAdmin(datasetInstanceId, classLoader);
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T extends DatasetAdmin> T getAdmin(DatasetId datasetInstanceId, @Nullable ClassLoader classLoader,
                                             DatasetClassLoaderProvider classLoaderProvider)
    throws DatasetManagementException, IOException, UnauthorizedException {
    // Return the no-op admin for the dataset from the real space
    if (actualDatasetFramework.hasInstance(datasetInstanceId)) {
      return (T) NOOP_DATASET_ADMIN;
    }
    return delegate.getAdmin(datasetInstanceId, classLoader, classLoaderProvider);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(final DatasetId datasetInstanceId,
                                          final Map<String, String> arguments,
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

      return DefaultDatasetRuntimeContext.execute(
        enforcer, NOOP_DATASET_ACCESS_RECORDER, principal, datasetInstanceId, null, () -> {
          if (isUserDataset && actualDatasetFramework.hasInstance(datasetInstanceId)) {
            return actualDatasetFramework.getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider,
                                                     owners, accessType);
          }
          return delegate.getDataset(datasetInstanceId, arguments, classLoader, classLoaderProvider,
                                     owners, accessType);
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
