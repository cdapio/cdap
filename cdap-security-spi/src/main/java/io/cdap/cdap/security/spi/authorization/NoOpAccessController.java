/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.security.spi.authorization;

import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;

import java.util.Collections;
import java.util.Set;

/**
 * A no-op authorizer to use when authorization is disabled.
 */
public class NoOpAccessController implements AccessController {

  @Override
  public void enforce(EntityId entity, Principal principal, Permission permission) {
    // no-op
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal) {
    return entityIds;
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions) {
    // no-op
  }

  @Override
  public void grant(Authorizable authorizable, Principal principal, Set<? extends Permission> permissions) {
    // no-op
  }

  @Override
  public void revoke(Authorizable authorizable, Principal principal, Set<? extends Permission> permissions) {
    // no-op
  }

  @Override
  public void revoke(Authorizable authorizable) {
    // no-op
  }

  @Override
  public void createRole(Role role) {
    //no-op
  }

  @Override
  public void dropRole(Role role) {
    //no-op
  }

  @Override
  public void addRoleToPrincipal(Role role, Principal principal) {
    //no-op
  }

  @Override
  public void removeRoleFromPrincipal(Role role, Principal principal) {
    //no-op
  }

  @Override
  public Set<Role> listRoles(Principal principal) {
    return Collections.emptySet();
  }

  @Override
  public Set<Role> listAllRoles() {
    return Collections.emptySet();
  }

  @Override
  public Set<GrantedPermission> listGrants(Principal principal) {
    return Collections.emptySet();
  }
}
