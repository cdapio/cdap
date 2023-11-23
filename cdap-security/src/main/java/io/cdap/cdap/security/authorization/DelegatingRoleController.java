/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.security.authorization;

import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessControllerSpi;
import io.cdap.cdap.security.spi.authorization.AuthorizationResponse;
import io.cdap.cdap.security.spi.authorization.AuthorizedResult;
import java.util.Set;

/**
 * A {@link RoleController} implements that delegates to the authorizer.
 * Having this makes Guice injection for RoleController manager simple.
 */
public class DelegatingRoleController implements RoleController {

  private final AccessControllerSpi delegateAccessController;
  private final AuthenticationContext authenticationContext;
  private final MetricsContext metricsContext;

  @Inject
  DelegatingRoleController(AccessControllerInstantiator accessControllerInstantiator,
                           AuthenticationContext authenticationContext,
                           SecurityMetricsService securityMetricsService) {
    this.delegateAccessController = accessControllerInstantiator.get();
    this.authenticationContext = authenticationContext;
    metricsContext = securityMetricsService.createEntityIdMetricsContext(InstanceId.SELF);
  }

  @Override
  public void createRole(Role role) throws AccessException {
    AuthorizationResponse authorizationResponse = delegateAccessController
      .createRole(authenticationContext.getPrincipal(), role);
    AuthorizationUtil.setAuthorizationDataInContext(authorizationResponse);
    AuthorizationUtil.throwIfUnauthorized(authorizationResponse);
    AuthorizationUtil.incrementCheckMetricExtension(metricsContext, authorizationResponse);
  }

  @Override
  public void dropRole(Role role) throws AccessException {
    AuthorizationResponse authorizationResponse = delegateAccessController
      .dropRole(authenticationContext.getPrincipal(), role);
    AuthorizationUtil.setAuthorizationDataInContext(authorizationResponse);
    AuthorizationUtil.throwIfUnauthorized(authorizationResponse);
    AuthorizationUtil.incrementCheckMetricExtension(metricsContext, authorizationResponse);
  }

  @Override
  public void addRoleToPrincipal(Role role, Principal principal) throws AccessException {
    AuthorizationResponse authorizationResponse = delegateAccessController
      .addRoleToPrincipal(authenticationContext.getPrincipal(), role, principal);
    AuthorizationUtil.setAuthorizationDataInContext(authorizationResponse);
    AuthorizationUtil.throwIfUnauthorized(authorizationResponse);
    AuthorizationUtil.incrementCheckMetricExtension(metricsContext, authorizationResponse);
  }

  @Override
  public void removeRoleFromPrincipal(Role role, Principal principal) throws AccessException {
    AuthorizationResponse authorizationResponse =
      delegateAccessController.removeRoleFromPrincipal(authenticationContext.getPrincipal(), role, principal);
    AuthorizationUtil.setAuthorizationDataInContext(authorizationResponse);
    AuthorizationUtil.throwIfUnauthorized(authorizationResponse);
    AuthorizationUtil.incrementCheckMetricExtension(metricsContext, authorizationResponse);
  }

  @Override
  public Set<Role> listRoles(Principal principal) throws AccessException {
    AuthorizedResult<Set<Role>> roleAuthResult = delegateAccessController
      .listRoles(authenticationContext.getPrincipal(), principal);
    AuthorizationUtil.setAuthorizationDataInContext(roleAuthResult.getAuthorizationResponse());
    AuthorizationUtil.throwIfUnauthorized(roleAuthResult.getAuthorizationResponse());
    AuthorizationUtil.incrementCheckMetricExtension(metricsContext, roleAuthResult.getAuthorizationResponse());
    return roleAuthResult.getResult();
  }

  @Override
  public Set<Role> listAllRoles() throws AccessException {
    AuthorizedResult<Set<Role>> roleAuthResult = delegateAccessController
      .listAllRoles(authenticationContext.getPrincipal());
    AuthorizationUtil.setAuthorizationDataInContext(roleAuthResult.getAuthorizationResponse());
    AuthorizationUtil.throwIfUnauthorized(roleAuthResult.getAuthorizationResponse());
    AuthorizationUtil.incrementCheckMetricExtension(metricsContext, roleAuthResult.getAuthorizationResponse());
    return roleAuthResult.getResult();
  }

  @Override
  public Set<GrantedPermission> listGrants(Principal principal) throws AccessException {
    AuthorizedResult<Set<GrantedPermission>> grantedPermissionResult =
      delegateAccessController.listGrants(authenticationContext.getPrincipal(), principal);
    AuthorizationUtil.setAuthorizationDataInContext(grantedPermissionResult.getAuthorizationResponse());
    AuthorizationUtil.throwIfUnauthorized(grantedPermissionResult.getAuthorizationResponse());
    AuthorizationUtil.incrementCheckMetricExtension(metricsContext, grantedPermissionResult.getAuthorizationResponse());
    return grantedPermissionResult.getResult();
  }
}
