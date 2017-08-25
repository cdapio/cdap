/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authorization.AuthorizationUtil;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.http.BodyConsumer;
import com.google.common.base.Function;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.name.Named;

import java.util.EnumSet;
import java.util.List;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * A class which has a delegation {@link DatasetTypeService} which has authorization enforce on the methods.
 */
public class AuthorizationDatasetTypeService extends AbstractIdleService implements DatasetTypeService {

  private final DatasetTypeService delegate;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  public AuthorizationDatasetTypeService(
    @Named(DataSetServiceModules.NOAUTH_DATASET_TYPE_SERVICE) DatasetTypeService datasetTypeService,
    AuthorizationEnforcer authorizationEnforcer,
    AuthenticationContext authenticationContext) {
    this.delegate = datasetTypeService;
    this.authorizationEnforcer = authorizationEnforcer;
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
    List<DatasetModuleMeta> modules = delegate.listModules(namespaceId);
    return AuthorizationUtil.isVisible(modules, authorizationEnforcer, authenticationContext.getPrincipal(),
                                       new Function<DatasetModuleMeta, EntityId>() {
                                         @Override
                                         public EntityId apply(DatasetModuleMeta input) {
                                           return namespaceId.datasetModule(input.getName());
                                         }
                                       }, null);
  }

  @Override
  public DatasetModuleMeta getModule(DatasetModuleId datasetModuleId) throws Exception {
    // No authorization for system modules
    if (!NamespaceId.SYSTEM.equals(datasetModuleId.getNamespaceId())) {
      AuthorizationUtil.ensureOnePrivilege(datasetModuleId, EnumSet.allOf(Action.class),
                                           authorizationEnforcer, authenticationContext.getPrincipal());
    }
    return delegate.getModule(datasetModuleId);
  }

  @Override
  public BodyConsumer addModule(DatasetModuleId datasetModuleId, String className,
                                boolean forceUpdate) throws Exception {
    final Principal principal = authenticationContext.getPrincipal();
    // enforce that the principal has ADMIN access on the dataset module
    authorizationEnforcer.enforce(datasetModuleId, principal, Action.ADMIN);
    return delegate.addModule(datasetModuleId, className, forceUpdate);
  }

  @Override
  public void delete(DatasetModuleId datasetModuleId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    authorizationEnforcer.enforce(datasetModuleId, principal, Action.ADMIN);
    delegate.delete(datasetModuleId);
  }

  @Override
  public void deleteAll(NamespaceId namespaceId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    for (DatasetModuleMeta meta : delegate.listModules(namespaceId)) {
      DatasetModuleId datasetModuleId = namespaceId.datasetModule(meta.getName());
      authorizationEnforcer.enforce(datasetModuleId, principal, Action.ADMIN);
    }
    delegate.deleteAll(namespaceId);
  }

  @Override
  public List<DatasetTypeMeta> listTypes(final NamespaceId namespaceId) throws Exception {
    List<DatasetTypeMeta> types = delegate.listTypes(namespaceId);
    return AuthorizationUtil.isVisible(types, authorizationEnforcer, authenticationContext.getPrincipal(),
                                       new Function<DatasetTypeMeta, EntityId>() {
                                         @Override
                                         public EntityId apply(DatasetTypeMeta input) {
                                           return namespaceId.datasetType(input.getName());
                                         }
                                       }, null);
  }

  @Override
  public DatasetTypeMeta getType(DatasetTypeId datasetTypeId) throws Exception {
    // No authorization for system dataset types
    if (!NamespaceId.SYSTEM.equals(datasetTypeId.getNamespaceId())) {
      AuthorizationUtil.ensureOnePrivilege(datasetTypeId, EnumSet.allOf(Action.class),
                                           authorizationEnforcer, authenticationContext.getPrincipal());
    }
    return delegate.getType(datasetTypeId);
  }
}
