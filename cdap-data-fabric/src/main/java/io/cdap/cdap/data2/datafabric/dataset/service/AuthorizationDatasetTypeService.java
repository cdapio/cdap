/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.service;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.name.Named;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.http.BodyConsumer;

import java.util.List;
import javax.inject.Inject;

/**
 * A class which has a delegation {@link DatasetTypeService} which has authorization enforce on the methods.
 */
public class AuthorizationDatasetTypeService extends AbstractIdleService implements DatasetTypeService {

  private final DatasetTypeService delegate;
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  public AuthorizationDatasetTypeService(
    @Named(DataSetServiceModules.NOAUTH_DATASET_TYPE_SERVICE) DatasetTypeService datasetTypeService,
    AccessEnforcer accessEnforcer,
    AuthenticationContext authenticationContext) {
    this.delegate = datasetTypeService;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
  }

  @Override
  protected void startUp() throws Exception {
    delegate.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    delegate.stopAndWait();
  }

  @Override
  public List<DatasetModuleMeta> listModules(final NamespaceId namespaceId) throws Exception {
    accessEnforcer.enforceOnParent(EntityType.DATASET_MODULE, namespaceId, authenticationContext.getPrincipal(),
                                   StandardPermission.LIST);
    return delegate.listModules(namespaceId);
  }

  @Override
  public DatasetModuleMeta getModule(DatasetModuleId datasetModuleId) throws Exception {
    // No authorization for system modules
    if (!NamespaceId.SYSTEM.equals(datasetModuleId.getNamespaceId())) {
      accessEnforcer.enforce(datasetModuleId, authenticationContext.getPrincipal(), StandardPermission.GET);
    }
    return delegate.getModule(datasetModuleId);
  }

  @Override
  public BodyConsumer addModule(DatasetModuleId datasetModuleId, String className,
                                boolean forceUpdate) throws Exception {
    final Principal principal = authenticationContext.getPrincipal();
    // enforce that the principal has CREATE access on the dataset module
    accessEnforcer.enforce(datasetModuleId, principal, StandardPermission.CREATE);
    return delegate.addModule(datasetModuleId, className, forceUpdate);
  }

  @Override
  public void delete(DatasetModuleId datasetModuleId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    accessEnforcer.enforce(datasetModuleId, principal, StandardPermission.DELETE);
    delegate.delete(datasetModuleId);
  }

  @Override
  public void deleteAll(NamespaceId namespaceId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    for (DatasetModuleMeta meta : delegate.listModules(namespaceId)) {
      DatasetModuleId datasetModuleId = namespaceId.datasetModule(meta.getName());
      accessEnforcer.enforce(datasetModuleId, principal, StandardPermission.DELETE);
    }
    delegate.deleteAll(namespaceId);
  }

  @Override
  public List<DatasetTypeMeta> listTypes(final NamespaceId namespaceId) throws Exception {
    accessEnforcer.enforceOnParent(EntityType.DATASET, namespaceId, authenticationContext.getPrincipal(),
                                   StandardPermission.LIST);
    return delegate.listTypes(namespaceId);
  }

  @Override
  public DatasetTypeMeta getType(DatasetTypeId datasetTypeId) throws Exception {
    // No authorization for system dataset types
    if (!NamespaceId.SYSTEM.equals(datasetTypeId.getNamespaceId())) {
      accessEnforcer.enforce(datasetTypeId, authenticationContext.getPrincipal(), StandardPermission.GET);
    }
    return delegate.getType(datasetTypeId);
  }
}
