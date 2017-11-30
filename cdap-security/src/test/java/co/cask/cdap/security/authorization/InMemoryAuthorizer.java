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

import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Authorizable;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.security.spi.authorization.AbstractAuthorizer;
import co.cask.cdap.security.spi.authorization.AlreadyExistsException;
import co.cask.cdap.security.spi.authorization.AuthorizationContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.NotFoundException;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory implementation of {@link Authorizer}.
 */
public class InMemoryAuthorizer extends AbstractAuthorizer {

  private final ConcurrentMap<Authorizable, ConcurrentMap<Principal, Set<Action>>> privileges =
    new ConcurrentHashMap<>();
  private final ConcurrentMap<Role, Set<Principal>> roleToPrincipals = new ConcurrentHashMap<>();
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
    // actions allowed for this principal
    Set<Action> allowed = getActions(entity, principal);
    if (allowed.containsAll(actions)) {
      return;
    }
    Set<Action> allowedForRoles = new HashSet<>();
    // actions allowed for any of the roles to which this principal belongs if its not a role
    if (principal.getType() != Principal.PrincipalType.ROLE) {
      for (Role role : getRoles(principal)) {
        allowedForRoles.addAll(getActions(entity, role));
      }
    }
    if (!allowedForRoles.containsAll(actions)) {
      throw new UnauthorizedException(principal, Sets.difference(actions, allowed), entity);
    }
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal) throws Exception {
    if (superUsers.contains(principal) || superUsers.contains(allSuperUsers)) {
      return entityIds;
    }
    Set<EntityId> results =  new HashSet<>();
    for (EntityId entityId : entityIds) {
      for (Authorizable existingEntity : privileges.keySet()) {
        if (isParent(entityId, existingEntity.getEntityParts())) {
          Set<Action> allowedActions = privileges.get(existingEntity).get(principal);
          if (allowedActions != null && !allowedActions.isEmpty()) {
            results.add(entityId);
            break;
          }
        }
      }
    }
    return results;
  }

  @Override
  public void grant(Authorizable authorizable, Principal principal, Set<Action> actions) throws Exception {
    getActions(authorizable, principal).addAll(actions);
  }

  @Override
  public void revoke(Authorizable authorizable, Principal principal, Set<Action> actions) throws Exception {
    getActions(authorizable, principal).removeAll(actions);
  }

  @Override
  public void revoke(Authorizable authorizable) throws Exception {
    privileges.remove(authorizable);
  }

  @Override
  public void createRole(Role role) throws AlreadyExistsException {
    if (roleToPrincipals.containsKey(role)) {
      throw new AlreadyExistsException(role);
    }
    // NOTE: A concurrent put might happen, hence it should still result as RoleAlreadyExistsException.
    Set<Principal> principals = Collections.newSetFromMap(new ConcurrentHashMap<Principal, Boolean>());
    if (roleToPrincipals.putIfAbsent(role, principals) != null) {
      throw new AlreadyExistsException(role);
    }
  }

  @Override
  public void dropRole(Role role) throws NotFoundException {
    Set<Principal> removed = roleToPrincipals.remove(role);
    if (removed == null) {
      throw new NotFoundException(role);
    }
  }

  @Override
  public void addRoleToPrincipal(Role role, Principal principal) throws NotFoundException {
    Set<Principal> principals = roleToPrincipals.get(role);
    if (principals == null) {
      throw new NotFoundException(role);
    }
    principals.add(principal);
  }

  @Override
  public void removeRoleFromPrincipal(Role role, Principal principal) throws NotFoundException {
    Set<Principal> principals = roleToPrincipals.get(role);
    if (principals == null) {
      throw new NotFoundException(role);
    }
    principals.remove(principal);
  }

  @Override
  public Set<Role> listRoles(Principal principal) {
    return Collections.unmodifiableSet(getRoles(principal));
  }

  @Override
  public Set<Role> listAllRoles() {
    return Collections.unmodifiableSet(roleToPrincipals.keySet());
  }

  @Override
  public Set<Privilege> listPrivileges(Principal principal) {
    Set<Privilege> privileges = new HashSet<>();
    // privileges for this principal
    privileges.addAll(getPrivileges(principal));

    // privileges for the role to which this principal belongs to if its not a role
    if (principal.getType() != Principal.PrincipalType.ROLE) {
      for (Role role : roleToPrincipals.keySet()) {
        privileges.addAll(getPrivileges(role));
      }
    }
    return Collections.unmodifiableSet(privileges);
  }

  private Set<Privilege> getPrivileges(Principal principal) {
    Set<Privilege> result = new HashSet<>();
    for (Map.Entry<Authorizable, ConcurrentMap<Principal, Set<Action>>> entry : privileges.entrySet()) {
      Authorizable authorizable = entry.getKey();
      Set<Action> actions = getActions(authorizable, principal);
      for (Action action : actions) {
        result.add(new Privilege(authorizable, action));
      }
    }
    return Collections.unmodifiableSet(result);
  }

  private Set<Action> getActions(EntityId entityId, Principal principal) {
    return getActions(Authorizable.fromEntityId(entityId), principal);
  }

  private Set<Action> getActions(Authorizable authorizable, Principal principal) {
    ConcurrentMap<Principal, Set<Action>> allActions = privileges.get(authorizable);
    if (allActions == null) {
      allActions = new ConcurrentHashMap<>();
      ConcurrentMap<Principal, Set<Action>> existingAllActions = privileges.putIfAbsent(authorizable, allActions);
      allActions = (existingAllActions == null) ? allActions : existingAllActions;
    }
    Set<Action> actions = allActions.get(principal);
    if (actions != null) {
      return actions;
    }

    actions = Collections.newSetFromMap(new ConcurrentHashMap<Action, Boolean>());
    Set<Action> existingActions = allActions.putIfAbsent(principal, actions);
    return existingActions == null ? actions : existingActions;
  }

  private Set<Role> getRoles(Principal principal) {
    Set<Role> roles = new HashSet<>();
    for (Map.Entry<Role, Set<Principal>> roleSetEntry : roleToPrincipals.entrySet()) {
      if (roleSetEntry.getValue().contains(principal)) {
        roles.add(roleSetEntry.getKey());
      }
    }
    return roles;
  }

  private boolean isParent(EntityId guessingParent, Map<EntityType, String> guessingChild) {
    Map<EntityType, String> questionedEntityParts = Authorizable.fromEntityId(guessingParent).getEntityParts();
    for (EntityType entityType : questionedEntityParts.keySet()) {
      if (!(guessingChild.containsKey(entityType) &&
        guessingChild.get(entityType).equals(questionedEntityParts.get(entityType)))) {
        return false;
      }
    }
    return true;
  }

  public final class AuthorizableEntityId {
    private final EntityId entityId;

    AuthorizableEntityId(EntityId entityId) {
      this.entityId = entityId;
    }

    public EntityId getEntityId() {
      return entityId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;

      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      AuthorizableEntityId that = (AuthorizableEntityId) o;
      if (!that.getEntityId().getEntityType().equals(entityId.getEntityType())) {
        return false;
      }

      EntityId thatEntityId = that.getEntityId();
      if (entityId.getEntityType().equals(EntityType.ARTIFACT)) {
        ArtifactId artifactId = (ArtifactId) entityId;
        ArtifactId thatArtifactId = (ArtifactId) thatEntityId;
        return Objects.equals(artifactId.getNamespace(), thatArtifactId.getNamespace()) &&
          Objects.equals(artifactId.getArtifact(), thatArtifactId.getArtifact());
      }
      if (entityId.getEntityType().equals(EntityType.APPLICATION)) {
        ApplicationId applicationId = (ApplicationId) entityId;
        ApplicationId thatApplicationId = (ApplicationId) thatEntityId;
        return Objects.equals(applicationId.getNamespace(), thatApplicationId.getNamespace()) &&
          Objects.equals(applicationId.getApplication(), thatApplicationId.getApplication());
      }
      if (entityId.getEntityType().equals(EntityType.PROGRAM)) {
        ProgramId programId = (ProgramId) entityId;
        ProgramId thatProgramId = (ProgramId) thatEntityId;
        return Objects.equals(programId.getNamespace(), thatProgramId.getNamespace()) &&
          Objects.equals(programId.getApplication(), thatProgramId.getApplication()) &&
          Objects.equals(programId.getType(), thatProgramId.getType()) &&
          Objects.equals(programId.getProgram(), thatProgramId.getProgram());
      }
      return Objects.equals(entityId, that.entityId);
    }

    @Override
    public int hashCode() {
      if (entityId.getEntityType().equals(EntityType.ARTIFACT)) {
        ArtifactId artifactId = (ArtifactId) entityId;
        return Objects.hash(artifactId.getEntityType(), artifactId.getNamespace(), artifactId.getArtifact());
      }
      if (entityId.getEntityType().equals(EntityType.APPLICATION)) {
        ApplicationId applicationId = (ApplicationId) entityId;
        return Objects.hash(applicationId.getEntityType(), applicationId.getNamespace(),
                            applicationId.getApplication());
      }
      if (entityId.getEntityType().equals(EntityType.PROGRAM)) {
        ProgramId programId = (ProgramId) entityId;
        return Objects.hash(programId.getEntityType(), programId.getNamespace(), programId.getApplication(),
                            programId.getType(), programId.getProgram());
      }
      return Objects.hash(entityId);
    }
  }
}
