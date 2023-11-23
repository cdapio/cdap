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

import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.security.spi.authorization.AlreadyExistsException;
import io.cdap.cdap.security.spi.authorization.NotFoundException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryRoleController implements RoleController {

  @Override
  public void createRole(Role role) throws AccessException {
    if (InMemoryPrivilegeHolder.getRoleToPrincipals().containsKey(role)) {
      throw new AlreadyExistsException(role);
    }
    // NOTE: A concurrent put might happen, hence it should still result as RoleAlreadyExistsException.
    Set<Principal> principals = Collections.newSetFromMap(new ConcurrentHashMap<Principal, Boolean>());
    if (InMemoryPrivilegeHolder.getRoleToPrincipals().putIfAbsent(role, principals) != null) {
      throw new AlreadyExistsException(role);
    }
  }

  @Override
  public void dropRole(Role role) throws AccessException {
    Set<Principal> removed = InMemoryPrivilegeHolder.getRoleToPrincipals().remove(role);
    if (removed == null) {
      throw new NotFoundException(role);
    }
  }

  @Override
  public void addRoleToPrincipal(Role role, Principal principal) throws AccessException {
    Set<Principal> principals = InMemoryPrivilegeHolder.getRoleToPrincipals().get(role);
    if (principals == null) {
      throw new NotFoundException(role);
    }
    principals.add(principal);
  }

  @Override
  public void removeRoleFromPrincipal(Role role, Principal principal) throws AccessException {
    Set<Principal> principals = InMemoryPrivilegeHolder.getRoleToPrincipals().get(role);
    if (principals == null) {
      throw new NotFoundException(role);
    }
    principals.remove(principal);
  }

  @Override
  public Set<Role> listRoles(Principal principal) throws AccessException {
    return Collections.unmodifiableSet(getRoles(principal));
  }

  @Override
  public Set<Role> listAllRoles() throws AccessException {
    return Collections.unmodifiableSet(InMemoryPrivilegeHolder.getRoleToPrincipals().keySet());
  }

  @Override
  public Set<GrantedPermission> listGrants(Principal principal) throws AccessException {
    Set<GrantedPermission> grantedPermissionSet = new HashSet<>();
    // privileges for this principal
    grantedPermissionSet.addAll(getPrivileges(principal));

    // privileges for the role to which this principal belongs to if its not a role
    if (principal.getType() != Principal.PrincipalType.ROLE) {
      for (Role role : InMemoryPrivilegeHolder.getRoleToPrincipals().keySet()) {
        grantedPermissionSet.addAll(getPrivileges(role));
      }
    }
    return grantedPermissionSet;
  }

  private Set<Role> getRoles(Principal principal) {
    Set<Role> roles = new HashSet<>();
    for (Map.Entry<Role, Set<Principal>> roleSetEntry : InMemoryPrivilegeHolder.getRoleToPrincipals().entrySet()) {
      if (roleSetEntry.getValue().contains(principal)) {
        roles.add(roleSetEntry.getKey());
      }
    }
    return roles;
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

    permissions = Collections.newSetFromMap(new ConcurrentHashMap<Permission, Boolean>());
    Set<Permission> existingPermissions = allPermissions.putIfAbsent(principal, permissions);
    return existingPermissions == null ? permissions : existingPermissions;
  }
}
