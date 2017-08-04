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
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.http.BodyConsumer;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import javax.inject.Inject;

/**
 * A class which has a delegation {@link DatasetTypeService} which has authorization enforce on the methods.
 */
public class AuthorizationDatasetTypeService extends AbstractIdleService implements DatasetTypeService {
  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationDatasetTypeService.class);

  private final DatasetTypeService delegate;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final PrivilegesManager privilegesManager;
  private final AuthenticationContext authenticationContext;

  @Inject
  public AuthorizationDatasetTypeService(
    @Named(DataSetServiceModules.BASE_DATASET_TYPE_SERVICE) DatasetTypeService datasetTypeService,
    AuthorizationEnforcer authorizationEnforcer,
    PrivilegesManager privilegesManager, AuthenticationContext authenticationContext) {
    this.delegate = datasetTypeService;
    this.authorizationEnforcer = authorizationEnforcer;
    this.privilegesManager = privilegesManager;
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
    final Principal principal = authenticationContext.getPrincipal();
    Iterable<DatasetModuleMeta> authorizedDatasetModules =
      Iterables.filter(modules, new com.google.common.base.Predicate<DatasetModuleMeta>() {
        @Override
        public boolean apply(DatasetModuleMeta datasetModuleMeta) {
          DatasetModuleId datasetModuleId = namespaceId.datasetModule(datasetModuleMeta.getName());
          try {
            return !authorizationEnforcer.isVisible(Collections.singleton(datasetModuleId), principal).isEmpty();
          } catch (Exception e) {
            LOG.warn("Error checking visibility for dataset module: {}", datasetModuleId);
            return false;
          }
        }
      });
    return Lists.newArrayList(authorizedDatasetModules);
  }

  @Override
  public DatasetModuleMeta getModule(DatasetModuleId datasetModuleId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    if (authorizationEnforcer.isVisible(Collections.singleton(datasetModuleId), principal).isEmpty()) {
      throw new UnauthorizedException(principal, datasetModuleId);
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
    // revoke all privileges on all modules
    for (DatasetModuleMeta meta : delegate.listModules(namespaceId)) {
      DatasetModuleId datasetModuleId = namespaceId.datasetModule(meta.getName());
      authorizationEnforcer.enforce(datasetModuleId, principal, Action.ADMIN);
    }
    delegate.deleteAll(namespaceId);
  }

  @Override
  public List<DatasetTypeMeta> listTypes(final NamespaceId namespaceId) throws Exception {
    List<DatasetTypeMeta> types = delegate.listTypes(namespaceId);
    final Principal principal = authenticationContext.getPrincipal();
    Iterable<DatasetTypeMeta> authorizedDatasetTypes =
      Iterables.filter(types, new com.google.common.base.Predicate<DatasetTypeMeta>() {
        @Override
        public boolean apply(DatasetTypeMeta datasetTypeMeta) {
          DatasetTypeId datasetTypeId = namespaceId.datasetType(datasetTypeMeta.getName());
          try {
            return !authorizationEnforcer.isVisible(Collections.singleton(datasetTypeId), principal).isEmpty();
          } catch (Exception e) {
            LOG.warn("Error checking visibility for dataset type: {}", datasetTypeId);
            return false;
          }
        }
      });
    return Lists.newArrayList(authorizedDatasetTypes);
  }

  @Override
  public DatasetTypeMeta getType(DatasetTypeId datasetTypeId) throws Exception {
    // All principals can access system dataset types
    // TODO: Test if this can be removed
    // only return the type if the user has some privileges on it
    Principal principal = authenticationContext.getPrincipal();
    if (!NamespaceId.SYSTEM.equals(datasetTypeId.getParent()) &&
      authorizationEnforcer.isVisible(Collections.singleton(datasetTypeId), principal).isEmpty()) {
      throw new UnauthorizedException(principal, datasetTypeId);
    }
    return delegate.getType(datasetTypeId);
  }
}
