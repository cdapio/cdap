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

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.security.spi.authorization.AccessControllerSpi;
import io.cdap.cdap.security.spi.authorization.AlreadyExistsException;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationResponse;
import io.cdap.cdap.security.spi.authorization.AuthorizedResult;
import io.cdap.cdap.security.spi.authorization.NotFoundException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * In-memory implementation of {@link AccessControllerSpi}.
 */
public class InMemoryAccessControllerV2 implements AccessControllerSpi {

  private final Set<Principal> superUsers = new HashSet<>();
  // Bypass enforcement for tests that want to simulate every user as a super user
  private final Principal allSuperUsers = new Principal("*", Principal.PrincipalType.USER);

  private final AuthorizationResponse authorizedResult = new AuthorizationResponse.Builder()
    .setAuthorized(AuthorizationResponse.AuthorizationStatus.AUTHORIZED)
    .setAuditLogContext(AuditLogContext.Builder.defaultNotRequired()).build();

  @Override
  public void initialize(AuthorizationContext context) {
    Properties properties = context.getExtensionProperties();
    if (properties.containsKey("superusers")) {
      for (String superuser : Splitter.on(",").trimResults().omitEmptyStrings()
        .split(properties.getProperty("superusers"))) {
        superUsers.add(new Principal(superuser, Principal.PrincipalType.USER));
      }
    }
  }


  @Override
  public AuthorizationResponse enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions)
    throws UnauthorizedException {
    return enforce(entity, null, principal, permissions);
  }

  private AuthorizationResponse enforce(EntityId entity, @Nullable EntityType childType,
            Principal principal, Set<? extends Permission> permissions) throws UnauthorizedException {
    // super users do not have any enforcement
    if (superUsers.contains(principal) || superUsers.contains(allSuperUsers)) {
      return AuthorizationResponse.Builder.defaultNotRequired();
    }
    // permissions allowed for this principal
    Set<? extends Permission> allowed = getPermissions(entity, childType, principal);
    if (allowed.containsAll(permissions)) {
      return authorizedResult;
    }
    Set<Permission> allowedForRoles = new HashSet<>();
    // permissions allowed for any of the roles to which this principal belongs if its not a role
    if (principal.getType() != Principal.PrincipalType.ROLE) {
      for (Role role : getRoles(principal)) {
        allowedForRoles.addAll(getPermissions(entity, role));
      }
    }
    if (!allowedForRoles.containsAll(permissions)) {
      throw new UnauthorizedException(principal, Sets.difference(permissions, allowed), entity, childType);
    }
    return authorizedResult;
  }

  @Override
  public AuthorizationResponse enforceOnParent(EntityType entityType, EntityId parentId, Principal principal,
                                               Permission permission) throws UnauthorizedException {
    return enforce(parentId, entityType, principal, Collections.singleton(permission));
  }

  @Override
  public Map<? extends EntityId, AuthorizationResponse> isVisible(Set<? extends EntityId> entityIds,
                                                                  Principal principal) {
    if (superUsers.contains(principal) || superUsers.contains(allSuperUsers)) {
      return entityIds.stream()
        .collect(Collectors.toMap(x -> x, x -> AuthorizationResponse.Builder.defaultNotRequired()));
    }
    Set<EntityId> results =  new HashSet<>();
    for (EntityId entityId : entityIds) {
      for (Authorizable existingEntity : InMemoryPrivilegeHolder.getPrivileges().keySet()) {
        if (isParent(entityId, existingEntity.getEntityParts())) {
          Set<? extends Permission> allowedPermissions = InMemoryPrivilegeHolder.getPrivileges()
            .get(existingEntity).get(principal);
          if (allowedPermissions != null && !allowedPermissions.isEmpty()) {
            results.add(entityId);
            break;
          }
        }
      }
    }
    return results.stream()
      .collect(Collectors.toMap(x -> x,
                                x -> authorizedResult));
  }

  @Override
  public AuthorizationResponse grant(Principal caller, Authorizable authorizable, Principal principal,
                                     Set<? extends Permission> permissions) {
    getPermissions(authorizable, principal).addAll(permissions);
    return authorizedResult;
  }

  @Override
  public AuthorizationResponse revoke(Principal caller, Authorizable authorizable, Principal principal,
                                      Set<? extends Permission> permissions) {
    getPermissions(authorizable, principal).removeAll(permissions);
    return authorizedResult;
  }

  @Override
  public AuthorizationResponse revoke(Principal caller, Authorizable authorizable) {
    InMemoryPrivilegeHolder.getPrivileges().remove(authorizable);
    return authorizedResult;
  }

  @Override
  public AuthorizationResponse createRole(Principal caller, Role role) throws AlreadyExistsException {
    if (InMemoryPrivilegeHolder.getRoleToPrincipals().containsKey(role)) {
      throw new AlreadyExistsException(role);
    }
    // NOTE: A concurrent put might happen, hence it should still result as RoleAlreadyExistsException.
    Set<Principal> principals = Collections.newSetFromMap(new ConcurrentHashMap<Principal, Boolean>());
    if (InMemoryPrivilegeHolder.getRoleToPrincipals().putIfAbsent(role, principals) != null) {
      throw new AlreadyExistsException(role);
    }
    return authorizedResult;
  }

  @Override
  public AuthorizationResponse dropRole(Principal caller, Role role) throws NotFoundException {
    Set<Principal> removed = InMemoryPrivilegeHolder.getRoleToPrincipals().remove(role);
    if (removed == null) {
      throw new NotFoundException(role);
    }
    return authorizedResult;
  }

  @Override
  public AuthorizationResponse addRoleToPrincipal(Principal caller, Role role, Principal principal)
  throws NotFoundException {
    Set<Principal> principals = InMemoryPrivilegeHolder.getRoleToPrincipals().get(role);
    if (principals == null) {
      throw new NotFoundException(role);
    }
    principals.add(principal);
    return authorizedResult;
  }

  @Override
  public AuthorizationResponse removeRoleFromPrincipal(Principal caller, Role role, Principal principal)
  throws NotFoundException {
    Set<Principal> principals = InMemoryPrivilegeHolder.getRoleToPrincipals().get(role);
    if (principals == null) {
      throw new NotFoundException(role);
    }
    principals.remove(principal);
    return new AuthorizationResponse.Builder()
      .setAuthorized(AuthorizationResponse.AuthorizationStatus.AUTHORIZED)
      .setAuditLogContext(AuditLogContext.Builder.defaultNotRequired()).build();
  }

  @Override
  public AuthorizedResult<Set<Role>> listRoles(Principal caller, Principal principal) {
    return new AuthorizedResult<>(Collections.unmodifiableSet(getRoles(principal)), authorizedResult);
  }

  @Override
  public AuthorizedResult<Set<Role>> listAllRoles(Principal caller) {
    return new AuthorizedResult<>(
      Collections.unmodifiableSet(InMemoryPrivilegeHolder.getRoleToPrincipals().keySet()), authorizedResult);
  }

  @Override
  public AuthorizedResult<Set<GrantedPermission>> listGrants(Principal caller, Principal principal) {
    Set<GrantedPermission> privileges = new HashSet<>();
    // privileges for this principal
    privileges.addAll(getPrivileges(principal));

    // privileges for the role to which this principal belongs to if its not a role
    if (principal.getType() != Principal.PrincipalType.ROLE) {
      for (Role role : InMemoryPrivilegeHolder.getRoleToPrincipals().keySet()) {
        privileges.addAll(getPrivileges(role));
      }
    }
    return new AuthorizedResult<>(privileges, authorizedResult);
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

    permissions = Collections.newSetFromMap(new ConcurrentHashMap<Permission, Boolean>());
    Set<Permission> existingPermissions = allPermissions.putIfAbsent(principal, permissions);
    return existingPermissions == null ? permissions : existingPermissions;
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

  private boolean isParent(EntityId guessingParent, Map<EntityType, String> guessingChild) {
    Map<EntityType, String> questionedEntityParts = Authorizable.fromEntityId(guessingParent).getEntityParts();
    for (EntityType entityType : questionedEntityParts.keySet()) {
      if (!(guessingChild.containsKey(entityType)
          && guessingChild.get(entityType).equals(questionedEntityParts.get(entityType)))) {
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
        return Objects.equals(artifactId.getNamespace(), thatArtifactId.getNamespace())
            && Objects.equals(artifactId.getArtifact(), thatArtifactId.getArtifact());
      }
      if (entityId.getEntityType().equals(EntityType.APPLICATION)) {
        ApplicationId applicationId = (ApplicationId) entityId;
        ApplicationId thatApplicationId = (ApplicationId) thatEntityId;
        return Objects.equals(applicationId.getNamespace(), thatApplicationId.getNamespace())
            && Objects.equals(applicationId.getApplication(), thatApplicationId.getApplication());
      }
      if (entityId.getEntityType().equals(EntityType.PROGRAM)) {
        ProgramId programId = (ProgramId) entityId;
        ProgramId thatProgramId = (ProgramId) thatEntityId;
        return Objects.equals(programId.getNamespace(), thatProgramId.getNamespace())
            && Objects.equals(programId.getApplication(), thatProgramId.getApplication())
            && Objects.equals(programId.getType(), thatProgramId.getType())
            && Objects.equals(programId.getProgram(), thatProgramId.getProgram());
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
