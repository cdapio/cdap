/*
 * Copyright © 2021 Cask Data, Inc.
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

import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Privilege;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.security.spi.AccessException;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.AuthorizationContext;
import io.cdap.cdap.security.spi.authorization.Authorizer;

import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;

/**
 * Wraps an {@link AccessController} and makes {@link Authorizer}
 * out of it.
 * TODO: remove after platform fully migrated to use AccessController directly
 */
public class AccessControllerWrapper extends AccessEnforcerWrapper implements Authorizer {
  private final AccessController accessController;

  @Inject
  public AccessControllerWrapper(AccessController accessController) {
    super(accessController);
    this.accessController = accessController;
  }

  @Override
  public void createRole(Role role) throws AccessException {
    accessController.createRole(role);
  }

  @Override
  public void dropRole(Role role) throws AccessException {
    accessController.dropRole(role);
  }

  @Override
  public void addRoleToPrincipal(Role role, Principal principal) throws AccessException {
    accessController.addRoleToPrincipal(role, principal);
  }

  @Override
  public void removeRoleFromPrincipal(Role role, Principal principal) throws AccessException {
    accessController.removeRoleFromPrincipal(role, principal);
  }

  @Override
  public Set<Role> listRoles(Principal principal) throws AccessException {
    return accessController.listRoles(principal);
  }

  @Override
  public Set<Role> listAllRoles() throws AccessException {
    return accessController.listAllRoles();
  }

  @Override
  public void revoke(Authorizable authorizable) throws AccessException {
    accessController.revoke(authorizable);
  }

  @Override
  public void isVisible(EntityId entityId, Principal principal) throws AccessException {
    accessController.isVisible(entityId, principal);
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal)
    throws AccessException {
    return accessController.isVisible(entityIds, principal);
  }

  @Override
  public Set<Privilege> listPrivileges(Principal principal) throws Exception {
    return accessController.listGrants(principal).stream()
      .map(p -> new Privilege(p.getAuthorizable(), AuthorizerWrapper.getAction(
        p.getPermission(), p.getAuthorizable().getEntityType())))
      .collect(Collectors.toSet());
  }

  @Override
  public void grant(Authorizable authorizable, Principal principal, Set<Action> actions) throws Exception {
    accessController.grant(authorizable, principal, getPermissions(actions));
  }

  @Override
  public void revoke(Authorizable authorizable, Principal principal, Set<Action> actions) throws Exception {
    accessController.revoke(authorizable, principal, getPermissions(actions));
  }

  @Override
  public void initialize(AuthorizationContext context) throws Exception {

  }

  @Override
  public void destroy() throws Exception {

  }
}
