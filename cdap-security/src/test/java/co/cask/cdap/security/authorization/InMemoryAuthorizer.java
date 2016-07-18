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

package co.cask.cdap.security.authorization;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.security.spi.authorization.AbstractAuthorizer;
import co.cask.cdap.security.spi.authorization.AuthorizationContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.RoleAlreadyExistsException;
import co.cask.cdap.security.spi.authorization.RoleNotFoundException;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Splitter;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * In-memory implementation of {@link Authorizer}.
 */
@NotThreadSafe
public class InMemoryAuthorizer extends AbstractAuthorizer {

  private final Table<EntityId, Principal, Set<Action>> table = HashBasedTable.create();
  private final Map<Role, Set<Principal>> roleToPrincipals = new HashMap<>();
  private final Set<Principal> superUsers = new HashSet<>();
  // Bypass enforcement for tests that want to simulate every user as a super user
  private final Principal allSuperUsers = new Principal("*", Principal.PrincipalType.USER);

  @Override
  public void initialize(AuthorizationContext context) throws Exception {
    Properties properties = context.getExtensionProperties();
    if (properties.containsKey("superusers")) {
      for (String superuser : Splitter.on(",").trimResults().omitEmptyStrings()
        .split(properties.getProperty("superusers"))) {
        superUsers.add(new Principal(superuser, Principal.PrincipalType.USER));
      }
    }
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<Action> actions) throws UnauthorizedException {
    // super users do not have any enforcement
    if (superUsers.contains(principal) || superUsers.contains(allSuperUsers)) {
      return;
    }
    // actions allowed to this principal
    Set<Action> allowed = Sets.newHashSet(getActions(entity, principal));
    // actions allowed to any of the roles to which this principal belongs if its not a role
    if (principal.getType() != Principal.PrincipalType.ROLE) {
      for (Role role : listRoles(principal)) {
        allowed.addAll(getActions(entity, role));
      }
    }
    if (!(allowed.contains(Action.ALL) || allowed.containsAll(actions))) {
      throw new UnauthorizedException(principal, Sets.difference(actions, allowed), entity);
    }
  }

  @Override
  public Predicate<EntityId> createFilter(Principal principal) throws Exception {
    // super users do not have any enforcement
    if (superUsers.contains(principal) || superUsers.contains(allSuperUsers)) {
      return ALLOW_ALL;
    }
    return super.createFilter(principal);
  }

  @Override
  public void grant(EntityId entity, Principal principal, Set<Action> actions) {
    getActions(entity, principal).addAll(actions);
  }

  @Override
  public void revoke(EntityId entity, Principal principal, Set<Action> actions) {
    getActions(entity, principal).removeAll(actions);
  }

  @Override
  public void revoke(EntityId entity) {
    for (Principal principal : table.row(entity).keySet()) {
      getActions(entity, principal).clear();
    }
  }

  @Override
  public void createRole(Role role) throws RoleAlreadyExistsException {
    if (roleToPrincipals.containsKey(role)) {
      throw new RoleAlreadyExistsException(role);
    }
    roleToPrincipals.put(role, new HashSet<Principal>());
  }

  @Override
  public void dropRole(Role role) throws RoleNotFoundException {
    if (!roleToPrincipals.containsKey(role)) {
      throw new RoleNotFoundException(role);
    }
    roleToPrincipals.remove(role);
  }

  @Override
  public void addRoleToPrincipal(Role role, Principal principal) throws RoleNotFoundException {
    if (!roleToPrincipals.containsKey(role)) {
      throw new RoleNotFoundException(role);
    }
    roleToPrincipals.get(role).add(principal);
  }

  @Override
  public void removeRoleFromPrincipal(Role role, Principal principal) throws RoleNotFoundException {
    if (!roleToPrincipals.containsKey(role)) {
      throw new RoleNotFoundException(role);
    }
    roleToPrincipals.get(role).remove(principal);
  }

  @Override
  public Set<Role> listRoles(Principal principal) {
    Set<Role> roles = new HashSet<>();
    for (Map.Entry<Role, Set<Principal>> roleSetEntry : roleToPrincipals.entrySet()) {
      if (roleSetEntry.getValue().contains(principal)) {
        roles.add(roleSetEntry.getKey());
      }
    }
    return roles;
  }

  @Override
  public Set<Role> listAllRoles() {
    return roleToPrincipals.keySet();
  }

  @Override
  public Set<Privilege> listPrivileges(Principal principal) {
    Set<Privilege> privileges = new HashSet<>();
    // privileges for this principal
    privileges.addAll(getPrivileges(principal));

    // privileges for the role to which this principal belongs to if its not a role
    if (principal.getType() != Principal.PrincipalType.ROLE) {
      for (Role role : listRoles(principal)) {
        privileges.addAll(getPrivileges(role));
      }
    }
    return privileges;
  }

  private Set<Privilege> getPrivileges(Principal principal) {
    Set<Privilege> privileges = new HashSet<>();
    Set<Map.Entry<EntityId, Set<Action>>> entries = table.column(principal).entrySet();
    for (Map.Entry<EntityId, Set<Action>> entry : entries) {
      for (Action action : entry.getValue()) {
        privileges.add(new Privilege(entry.getKey(), action));
      }
    }
    return privileges;
  }

  private Set<Action> getActions(EntityId entity, Principal principal) {
    if (!table.contains(entity, principal)) {
      table.put(entity, principal, new HashSet<Action>());
    }
    return table.get(entity, principal);
  }
}
