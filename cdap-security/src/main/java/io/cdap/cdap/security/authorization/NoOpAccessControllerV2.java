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
import io.cdap.cdap.security.spi.authorization.AccessControllerSpi;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationResponse;
import io.cdap.cdap.security.spi.authorization.AuthorizedResult;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A no-op authorizer to use when authorization is disabled.
 * This is the Version 2 of {@link io.cdap.cdap.security.spi.authorization.NoOpAccessController} to
 * implement from new {@link AccessControllerSpi}
 */
public class NoOpAccessControllerV2 implements AccessControllerSpi {

  @Override
  public Map<? extends EntityId, AuthorizationResponse> isVisible(Set<? extends EntityId> entityIds,
                                                                  Principal principal) {
    return entityIds.stream()
      .collect(Collectors.toMap(r -> r, r -> AuthorizationResponse.Builder.defaultNotRequired()));
  }

  @Override
  @Nullable
  public AuthorizationResponse enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions) {
    // no-op
    return AuthorizationResponse.Builder.defaultNotRequired();
  }

  @Override
  @Nullable
  public AuthorizationResponse enforceOnParent(EntityType entityType, EntityId parentId, Principal principal,
                                               Permission permission) {
    // no-op
    return AuthorizationResponse.Builder.defaultNotRequired();
  }

  @Override
  public AuthorizationResponse grant(Principal caller, Authorizable authorizable, Principal principal,
                                     Set<? extends Permission> permissions) {
    // no-op
    return AuthorizationResponse.Builder.defaultNotRequired();
  }

  @Override
  public AuthorizationResponse revoke(Principal caller, Authorizable authorizable, Principal principal,
                                      Set<? extends Permission> permissions) {
    // no-op
    return AuthorizationResponse.Builder.defaultNotRequired();
  }

  @Override
  public AuthorizationResponse revoke(Principal caller, Authorizable authorizable) {
    // no-op
    return AuthorizationResponse.Builder.defaultNotRequired();
  }

  @Override
  public AuthorizationResponse createRole(Principal caller, Role role) {
    //no-op
    return AuthorizationResponse.Builder.defaultNotRequired();
  }

  @Override
  public AuthorizationResponse dropRole(Principal caller, Role role) {
    //no-op
    return AuthorizationResponse.Builder.defaultNotRequired();
  }

  @Override
  public AuthorizationResponse addRoleToPrincipal(Principal caller, Role role, Principal principal) {
    //no-op
    return AuthorizationResponse.Builder.defaultNotRequired();
  }

  @Override
  public AuthorizationResponse removeRoleFromPrincipal(Principal caller, Role role, Principal principal) {
    //no-op
    return AuthorizationResponse.Builder.defaultNotRequired();
  }

  @Override
  public AuthorizedResult<Set<Role>> listRoles(Principal caller, Principal principal) {
    return new AuthorizedResult<>(Collections.emptySet(), AuthorizationResponse.Builder.defaultNotRequired());
  }

  @Override
  public AuthorizedResult<Set<Role>> listAllRoles(Principal caller) {
    return new AuthorizedResult<>(Collections.emptySet(), AuthorizationResponse.Builder.defaultNotRequired());
  }

  @Override
  public AuthorizedResult<Set<GrantedPermission>> listGrants(Principal principal, Principal caller) {
    return new AuthorizedResult<>(Collections.emptySet(), AuthorizationResponse.Builder.defaultNotRequired());
  }

  /**
   * TODO : THIS IS WIP : Needs to be modified based on how auth extension works.
   *
   * @return {@link PublishStatus}
   */
  @Override
  public PublishStatus publish(Queue<AuditLogContext> auditLogContexts) {
    //no-op
    return PublishStatus.PUBLISHED;
  }
}
