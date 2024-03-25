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
import io.cdap.cdap.common.security.AuthEnforceUtil;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.AccessControllerSpi;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationResponse;
import io.cdap.cdap.security.spi.authorization.AuthorizedResult;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Wraps an {@link AccessController} and creates an {@link AccessControllerSpi} out of it.
 */
public class AccessControllerWrapper implements AccessControllerSpi {

  private static final Logger LOG = LoggerFactory.getLogger(AccessControllerWrapper.class);
  private final AccessController accessControllerV1;

  public AccessControllerWrapper(AccessController accessControllerV1) {
    this.accessControllerV1 = accessControllerV1;
  }

  @Override
  public void initialize(AuthorizationContext context) {
    accessControllerV1.initialize(context);
  }

  @Override
  public AuthorizationResponse createRole(Principal caller, Role role) throws AccessException {
    try {
      accessControllerV1.createRole(role);
      return getAuthorizedResponse();
    } catch (UnauthorizedException e) {
        return createAuthResultFromUnauthorizedExp(e);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public AuthorizationResponse dropRole(Principal caller, Role role) throws AccessException {
    try {
      accessControllerV1.dropRole(role);
      return getAuthorizedResponse();
    } catch (UnauthorizedException e) {
      return createAuthResultFromUnauthorizedExp(e);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public AuthorizationResponse addRoleToPrincipal(Principal caller, Role role, Principal principal)
    throws AccessException {
    try {
      accessControllerV1.addRoleToPrincipal(role, principal);
      return getAuthorizedResponse();
    } catch (UnauthorizedException e) {
      return createAuthResultFromUnauthorizedExp(e);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public AuthorizationResponse removeRoleFromPrincipal(Principal caller, Role role, Principal principal)
    throws AccessException {
    try {
      accessControllerV1.removeRoleFromPrincipal(role, principal);
      return getAuthorizedResponse();
    } catch (UnauthorizedException e) {
      return createAuthResultFromUnauthorizedExp(e);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public AuthorizedResult<Set<Role>> listRoles(Principal caller, Principal principal) throws AccessException {
    try {
      Set<Role> roleSet = accessControllerV1.listRoles(principal);
      return new AuthorizedResult<>(roleSet, getAuthorizedResponse());
    } catch (UnauthorizedException e) {
      //Incase of UnauthorizedException , the set will be empty
      return new AuthorizedResult<>(Collections.emptySet(), createAuthResultFromUnauthorizedExp(e));
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public AuthorizedResult<Set<Role>> listAllRoles(Principal caller) throws AccessException {
    try {
      Set<Role> roleSet = accessControllerV1.listAllRoles();
      return new AuthorizedResult<>(roleSet, getAuthorizedResponse());
    } catch (UnauthorizedException e) {
      return new AuthorizedResult<>(Collections.emptySet(), createAuthResultFromUnauthorizedExp(e));
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public void destroy() {
      accessControllerV1.destroy();
  }

  @Override
  public AuthorizedResult<Set<GrantedPermission>> listGrants(Principal principal, Principal caller)
    throws AccessException {
    try {
      Set<GrantedPermission> grantedPermissionSet = accessControllerV1.listGrants(principal);
      return new AuthorizedResult<>(grantedPermissionSet, getAuthorizedResponse());
    } catch (UnauthorizedException e) {
      return new AuthorizedResult<>(Collections.emptySet(), createAuthResultFromUnauthorizedExp(e));
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public AuthorizationResponse grant(Principal caller, Authorizable authorizable, Principal principal,
                                     Set<? extends Permission> permissions) throws AccessException {

    try {
      accessControllerV1.grant(authorizable, principal, permissions);
      return getAuthorizedResponse();
    } catch (UnauthorizedException e) {
      return createAuthResultFromUnauthorizedExp(e);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public AuthorizationResponse revoke(Principal caller, Authorizable authorizable, Principal principal,
                                      Set<? extends Permission> permissions) throws AccessException {

    try {
      accessControllerV1.revoke(authorizable, principal, permissions);
      return getAuthorizedResponse();
    } catch (UnauthorizedException e) {
      return createAuthResultFromUnauthorizedExp(e);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public AuthorizationResponse revoke(Principal caller, Authorizable authorizable) throws AccessException {
    try {
      accessControllerV1.revoke(authorizable);
      return getAuthorizedResponse();
    } catch (UnauthorizedException e) {
      return createAuthResultFromUnauthorizedExp(e);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public AuthorizationResponse enforce(EntityId entity, Principal principal, Permission permission)
      throws AccessException {
    try {
      accessControllerV1.enforce(entity, principal, permission);
      return getAuthorizedResponse();
    } catch (UnauthorizedException e) {
      return createAuthResultFromUnauthorizedExp(e);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public AuthorizationResponse enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions)
      throws AccessException {
    try {
      accessControllerV1.enforce(entity, principal, permissions);
      return getAuthorizedResponse();
    } catch (UnauthorizedException e) {
      return createAuthResultFromUnauthorizedExp(e);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public AuthorizationResponse enforceOnParent(EntityType entityType, EntityId parentId, Principal principal,
                                               Permission permission) throws AccessException {
    try {
      accessControllerV1.enforceOnParent(entityType, parentId, principal, permission);
      return getAuthorizedResponse();
    } catch (UnauthorizedException e) {
      return createAuthResultFromUnauthorizedExp(e);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public Map<? extends EntityId, AuthorizationResponse> isVisible(Set<? extends EntityId> entityIds,
                                                                  Principal principal) throws AccessException {
    try {
      Set<? extends EntityId> entityIdsSet = accessControllerV1.isVisible(entityIds, principal);
      return entityIdsSet
        .stream().collect(Collectors.toMap(x -> x, x -> getAuthorizedResponse()));
    } catch (UnauthorizedException e) {
      //Incase of UnauthorizedException , the Map will be empty
      return entityIds.stream()
        .collect(Collectors.toMap(x -> x,
             x -> createAuthResultFromUnauthorizedExp(e)));
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  private AuthorizationResponse getAuthorizedResponse() {
    return  new AuthorizationResponse.Builder()
      .setAuthorized(AuthorizationResponse.AuthorizationStatus.AUTHORIZED)
      .setAuditLogContext(AuditLogContext.Builder.defaultNotRequired()).build();
  }

  private AuthorizationResponse createAuthResultFromUnauthorizedExp(UnauthorizedException e) {
    return  new AuthorizationResponse.Builder()
      .setAuditLogContext(AuditLogContext.Builder.defaultNotRequired())
      .setAuthorized(AuthorizationResponse.AuthorizationStatus.UNAUTHORIZED)
      .setPrincipal(e.getPrincipal())
      .setMissingPermissions(e.getMissingPermissions())
      .setEntity(e.getEntity())
      .setIncludePrincipal(e.includePrincipal())
      .setAddendum(e.getAddendum())
      .build();
  }

  @Override
  public PublishStatus publish(List<String> auditLogList) {
    LOG.warn("SANKET_LOG , not yet supported");
    // TODO : throw exception ? or log unsupported ?
    return PublishStatus.PUBLISHED;
  }
}
