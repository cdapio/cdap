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

import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.security.spi.authorization.PermissionManager;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory implementation of {@link PermissionManager}.
 */
public class InMemoryPermissionManager implements PermissionManager {

  @Override
  public void grant(Authorizable authorizable, Principal principal, Set<? extends Permission> permissions) {
    getPermissions(authorizable, principal).addAll(permissions);
  }

  @Override
  public void revoke(Authorizable authorizable, Principal principal, Set<? extends Permission> permissions) {
    getPermissions(authorizable, principal).removeAll(permissions);
  }

  @Override
  public void revoke(Authorizable authorizable) {
    InMemoryPrivilegeHolder.getPrivileges().remove(authorizable);
  }

  @Override
  public Set<GrantedPermission> listGrants(Principal principal) {
    Set<GrantedPermission> privileges = new HashSet<>();
    // privileges for this principal
    privileges.addAll(getPrivileges(principal));

    // privileges for the role to which this principal belongs to if its not a role
    if (principal.getType() != Principal.PrincipalType.ROLE) {
      for (Role role : InMemoryPrivilegeHolder.getRoleToPrincipals().keySet()) {
        privileges.addAll(getPrivileges(role));
      }
    }
    return Collections.unmodifiableSet(privileges);
  }

  private Set<GrantedPermission> getPrivileges(Principal principal) {
    Set<GrantedPermission> result = new HashSet<>();
    for (Map.Entry<Authorizable, ConcurrentMap<Principal, Set<Permission>>> entry :
      InMemoryPrivilegeHolder.getPrivileges().entrySet()) {
      Authorizable authorizable = entry.getKey();
      Set<? extends Permission> permissions = getPermissions(authorizable, principal);
      for (Permission permission : permissions) {
        result.add(new GrantedPermission(authorizable, permission));
      }
    }
    return Collections.unmodifiableSet(result);
  }

  private Set<? extends Permission> getPermissions(EntityId entityId, Principal principal) {
    return getPermissions(Authorizable.fromEntityId(entityId), principal);
  }

  private Set<? extends Permission> getPermissions(EntityId entityId, EntityType childType, Principal principal) {
    return getPermissions(Authorizable.fromEntityId(entityId, childType), principal);
  }

  private Set<Permission> getPermissions(Authorizable authorizable, Principal principal) {
    ConcurrentMap<Principal, Set<Permission>> allPermissions =
      InMemoryPrivilegeHolder.getPrivileges().get(authorizable);
    if (allPermissions == null) {
      allPermissions = new ConcurrentHashMap<>();
      ConcurrentMap<Principal, Set<Permission>> existingAllPermissions =
        InMemoryPrivilegeHolder.getPrivileges().putIfAbsent(authorizable, allPermissions);
      allPermissions = (existingAllPermissions == null) ? allPermissions : existingAllPermissions;
    }
    Set<Permission> permissions = allPermissions.get(principal);
    if (permissions != null) {
      return permissions;
    }

    permissions = Collections.newSetFromMap(new ConcurrentHashMap<>());
    Set<Permission> existingPermissions = allPermissions.putIfAbsent(principal, permissions);
    return existingPermissions == null ? permissions : existingPermissions;
  }
}
