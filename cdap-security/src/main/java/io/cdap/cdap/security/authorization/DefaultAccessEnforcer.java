/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Security.Encryption;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.security.encryption.AeadCipher;
import io.cdap.cdap.security.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.AuthorizationResponse;
import io.cdap.cdap.security.spi.authorization.AuthorizedResult;
import io.cdap.cdap.security.spi.encryption.CipherException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link io.cdap.cdap.security.spi.authorization.AccessEnforcer} that runs on
 * the master. It calls the access controller directly to enforce authorization policies.
 */
@Singleton
public class DefaultAccessEnforcer extends AbstractAccessEnforcer implements RoleController {

  public static final String INTERNAL_ACCESS_ENFORCER = "internal";

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAccessEnforcer.class);

  private final SConfiguration sConf;
  private final AccessControllerInstantiator accessControllerInstantiator;
  @Nullable
  private final Principal masterUser;
  private final int logTimeTakenAsWarn;
  private final AccessEnforcer internalAccessEnforcer;
  private final boolean internalAuthEnabled;
  private final AeadCipher userEncryptionAeadCipher;
  private final AuthenticationContext authenticationContext;
  private final SecurityMetricsService securityMetricsService;

  @Inject
  DefaultAccessEnforcer(CConfiguration cConf, SConfiguration sConf,
      AccessControllerInstantiator accessControllerInstantiator,
      @Named(INTERNAL_ACCESS_ENFORCER) AccessEnforcer internalAccessEnforcer,
      @Named(UserCredentialAeadEncryptionModule.USER_CREDENTIAL_ENCRYPTION) AeadCipher userEncryptionAeadCipher,
      AuthenticationContext authenticationContext,
      SecurityMetricsService securityMetricsService) {
    super(cConf);
    this.sConf = sConf;
    this.accessControllerInstantiator = accessControllerInstantiator;
    String masterUserName = AuthorizationUtil.getEffectiveMasterUser(cConf);
    this.masterUser =
        masterUserName == null ? null : new Principal(masterUserName, Principal.PrincipalType.USER);
    this.logTimeTakenAsWarn = cConf.getInt(
        Constants.Security.Authorization.EXTENSION_OPERATION_TIME_WARN_THRESHOLD);
    this.internalAccessEnforcer = internalAccessEnforcer;
    this.internalAuthEnabled = SecurityUtil.isInternalAuthEnabled(cConf);
    this.userEncryptionAeadCipher = userEncryptionAeadCipher;
    this.authenticationContext = authenticationContext;
    this.securityMetricsService = securityMetricsService;
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions) {
    MetricsContext metricsContext = securityMetricsService.createEntityIdMetricsContext(entity);
    AuthorizationResponse authorizationResponse;
    if (internalAuthEnabled && principal.getFullCredential() != null
        && principal.getFullCredential().getType() == Credential.CredentialType.INTERNAL) {
      LOG.trace("Internal Principal enforce({}, {}, {})", entity, principal, permissions);
      try {
        internalAccessEnforcer.enforce(entity, principal, permissions);
        metricsContext.increment(Constants.Metrics.Authorization.INTERNAL_CHECK_SUCCESS_COUNT, 1);
      } catch (Throwable e) {
        metricsContext.increment(Constants.Metrics.Authorization.INTERNAL_CHECK_FAILURE_COUNT, 1);
        throw e;
      }
      return;
    }
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }
    // bypass the check when the principal is the master user and the entity is in the system namespace
    if (isAccessingSystemNsasMasterUser(entity, principal) || isEnforcingOnSamePrincipalId(entity,
                                                                                           principal)) {
      metricsContext.increment(Constants.Metrics.Authorization.EXTENSION_CHECK_BYPASS_COUNT, 1);
      return;
    }

    principal = getUserPrinciple(principal);

    LOG.trace("Enforcing permissions {} on {} for principal {}.", permissions, entity, principal);
    long startTime = System.nanoTime();
    try {
      authorizationResponse = accessControllerInstantiator.get().enforce(entity, principal, permissions);
      AuthorizationUtil.incrementCheckMetricExtension(metricsContext, authorizationResponse);
      AuthorizationUtil.setAuthorizationDataInContext(authorizationResponse);
      AuthorizationUtil.throwIfUnauthorized(authorizationResponse);
    } catch (Throwable e) {
      metricsContext.increment(Constants.Metrics.Authorization.EXTENSION_CHECK_FAILURE_COUNT, 1);
      throw e;
    } finally {
      long timeTaken = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
      metricsContext.gauge(Constants.Metrics.Authorization.EXTENSION_CHECK_MILLIS, timeTaken);
      String logLine = "Enforced permissions {} on {} for principal {}. Time spent in enforcement was {} ms.";
      if (timeTaken > logTimeTakenAsWarn) {
        LOG.warn(logLine, permissions, entity, principal, timeTaken);
      } else {
        LOG.trace(logLine, permissions, entity, principal, timeTaken);
      }
    }
  }

  @Override
  public void enforceOnParent(EntityType entityType, EntityId parentId, Principal principal,
      Permission permission)
      throws AccessException {
    MetricsContext metricsContext = securityMetricsService.createEntityIdMetricsContext(parentId);
    AuthorizationResponse authorizationResponse;
    if (internalAuthEnabled && principal.getFullCredential() != null
        && principal.getFullCredential().getType() == Credential.CredentialType.INTERNAL) {
      LOG.trace("Internal Principal enforceOnParent({}, {}, {})", parentId, principal, permission);
      try {
        internalAccessEnforcer.enforceOnParent(entityType, parentId, principal, permission);
        metricsContext.increment(Constants.Metrics.Authorization.INTERNAL_CHECK_SUCCESS_COUNT, 1);
      } catch (Throwable e) {
        metricsContext.increment(Constants.Metrics.Authorization.INTERNAL_CHECK_FAILURE_COUNT, 1);
        throw e;
      }
      return;
    }

    if (!isSecurityAuthorizationEnabled()) {
      return;
    }

    // bypass the check when the principal is the master user and the entity is in the system namespace
    if (isAccessingSystemNsasMasterUser(parentId, principal) || isEnforcingOnSamePrincipalId(
        parentId, principal)) {
      metricsContext.increment(Constants.Metrics.Authorization.EXTENSION_CHECK_BYPASS_COUNT, 1);
      return;
    }

    principal = getUserPrinciple(principal);

    LOG.trace("Enforcing permission {} on {} in {} for principal {}.", permission, entityType,
        parentId, principal);
    long startTime = System.nanoTime();
    try {
      authorizationResponse = accessControllerInstantiator.get()
          .enforceOnParent(entityType, parentId, principal, permission);
      AuthorizationUtil.incrementCheckMetricExtension(metricsContext, authorizationResponse);
      AuthorizationUtil.setAuthorizationDataInContext(authorizationResponse);
      AuthorizationUtil.throwIfUnauthorized(authorizationResponse);
    } catch (Throwable e) {
      metricsContext.increment(Constants.Metrics.Authorization.EXTENSION_CHECK_FAILURE_COUNT, 1);
      throw e;
    } finally {
      long timeTaken = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
      metricsContext.gauge(Constants.Metrics.Authorization.EXTENSION_CHECK_MILLIS, timeTaken);
      String logLine = "Enforced permission {} on {} in {} for principal {}. Time spent in enforcement was {} ms.";
      if (timeTaken > logTimeTakenAsWarn) {
        LOG.warn(logLine, permission, entityType, parentId, principal, timeTaken);
      } else {
        LOG.trace(logLine, permission, entityType, parentId, principal, timeTaken);
      }
    }
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal)
      throws AccessException {
    // Pass null for creating metrics context. Aggregations are not supported for visibility checks.
    MetricsContext metricsContext = securityMetricsService.createEntityIdMetricsContext(null);
    if (internalAuthEnabled && principal.getFullCredential() != null
        && principal.getFullCredential().getType() == Credential.CredentialType.INTERNAL) {
      LOG.trace("Internal Principal enforce({}, {})", entityIds, principal);
      metricsContext.increment(Constants.Metrics.Authorization.INTERNAL_VISIBILITY_CHECK_COUNT, 1);
      return internalAccessEnforcer.isVisible(entityIds, principal);
    }

    if (!isSecurityAuthorizationEnabled()) {
      return entityIds;
    }

    metricsContext.increment(Constants.Metrics.Authorization.NON_INTERNAL_VISIBILITY_CHECK_COUNT,
        1);

    Set<EntityId> visibleEntities = new HashSet<>();
    // filter out entity id which is in system namespace and principal is the master user
    for (EntityId entityId : entityIds) {
      if (isAccessingSystemNsasMasterUser(entityId, principal) || isEnforcingOnSamePrincipalId(
          entityId, principal)) {
        visibleEntities.add(entityId);
      }
    }

    principal = getUserPrinciple(principal);

    Set<? extends EntityId> difference = Sets.difference(entityIds, visibleEntities);
    LOG.trace("Checking visibility of {} for principal {}.", difference, principal);
    Set<? extends EntityId> moreVisibleEntities;
    long startTime = System.nanoTime();
    try {
      Map<? extends EntityId, AuthorizationResponse> mapOfEntityResult =
        accessControllerInstantiator.get().isVisible(difference, principal);
      List<AuthorizationResponse> checkResults = mapOfEntityResult.values().stream()
        .collect(Collectors.toList());
      AuthorizationUtil.setAuthorizationDataInContext(checkResults);
      moreVisibleEntities = mapOfEntityResult.entrySet()
          .stream()
          .filter(entry -> entry.getValue().isAuthorized() != AuthorizationResponse.AuthorizationStatus.UNAUTHORIZED)
          .map(Map.Entry::getKey)
          .collect(Collectors.toSet());
    } finally {
      long timeTaken = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
      metricsContext.gauge(Constants.Metrics.Authorization.EXTENSION_VISIBILITY_MILLIS, timeTaken);
      String logLine = "Checked visibility of {} for principal {}. Time spent in visibility check was {} ms.";
      if (timeTaken > logTimeTakenAsWarn) {
        LOG.warn(logLine, difference, principal, timeTaken);
      } else {
        LOG.trace(logLine, difference, principal, timeTaken);
      }
    }
    visibleEntities.addAll(moreVisibleEntities);
    LOG.trace("Getting {} as visible entities", visibleEntities);
    return Collections.unmodifiableSet(visibleEntities);
  }

  private Principal getUserPrinciple(Principal principal) throws AccessException {
    if (principal.getFullCredential() == null
        || !sConf.getBoolean(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_ENABLED,
        false)) {
      return principal;
    }

    Credential userCredential = principal.getFullCredential();
    if (userCredential == null || !userCredential.getType()
        .equals(Credential.CredentialType.EXTERNAL_ENCRYPTED)) {
      return principal;
    }

    // When user credential encryption is enabled, credential should be encrypted upon arrival
    // at router and decrypted right here before calling auth extension.
    try {
      String plainCredential = userEncryptionAeadCipher
          .decryptStringFromBase64(userCredential.getValue(),
              Encryption.USER_CREDENTIAL_ENCRYPTION_ASSOCIATED_DATA.getBytes());
      return new Principal(principal.getName(),
          principal.getType(),
          principal.getKerberosPrincipal(),
          new Credential(plainCredential, Credential.CredentialType.EXTERNAL));
    } catch (CipherException e) {
      throw new AccessException("Failed to decrypt credential in principle: " + e.getMessage(), e);
    }
  }


  private boolean isAccessingSystemNsasMasterUser(EntityId entityId, Principal principal) {
    return entityId instanceof NamespacedEntityId
        && ((NamespacedEntityId) entityId).getNamespaceId().equals(NamespaceId.SYSTEM)
        && principal.equals(masterUser);
  }

  private boolean isEnforcingOnSamePrincipalId(EntityId entityId, Principal principal) {
    return entityId.getEntityType().equals(EntityType.KERBEROSPRINCIPAL)
        && principal.getName().equals(entityId.getEntityName());
  }

  @Override
  public void createRole(Role role) throws AccessException {
    AuthorizationResponse authorizationResponse = accessControllerInstantiator.get()
      .createRole(authenticationContext.getPrincipal(), role);
    AuthorizationUtil.setAuthorizationDataInContext(authorizationResponse);
    AuthorizationUtil.throwIfUnauthorized(authorizationResponse);
    AuthorizationUtil
      .incrementCheckMetricExtension(securityMetricsService.createEntityIdMetricsContext(InstanceId.SELF),
                                     authorizationResponse);
  }

  @Override
  public void dropRole(Role role) throws AccessException {
    AuthorizationResponse authorizationResponse = accessControllerInstantiator.get()
      .dropRole(authenticationContext.getPrincipal(), role);
    AuthorizationUtil.setAuthorizationDataInContext(authorizationResponse);
    AuthorizationUtil.throwIfUnauthorized(authorizationResponse);
    AuthorizationUtil
      .incrementCheckMetricExtension(
        securityMetricsService.createEntityIdMetricsContext(InstanceId.SELF), authorizationResponse);
  }

  @Override
  public void addRoleToPrincipal(Role role, Principal principal) throws AccessException {
    AuthorizationResponse authorizationResponse = accessControllerInstantiator.get()
      .addRoleToPrincipal(authenticationContext.getPrincipal(), role, principal);
    AuthorizationUtil.setAuthorizationDataInContext(authorizationResponse);
    AuthorizationUtil.throwIfUnauthorized(authorizationResponse);
    AuthorizationUtil
      .incrementCheckMetricExtension(
        securityMetricsService.createEntityIdMetricsContext(InstanceId.SELF), authorizationResponse);
  }

  @Override
  public void removeRoleFromPrincipal(Role role, Principal principal) throws AccessException {
    AuthorizationResponse authorizationResponse =
      accessControllerInstantiator.get().removeRoleFromPrincipal(authenticationContext.getPrincipal(), role, principal);
    AuthorizationUtil.setAuthorizationDataInContext(authorizationResponse);
    AuthorizationUtil.throwIfUnauthorized(authorizationResponse);
    AuthorizationUtil
      .incrementCheckMetricExtension(
        securityMetricsService.createEntityIdMetricsContext(InstanceId.SELF), authorizationResponse);
  }

  @Override
  public Set<Role> listRoles(Principal principal) throws AccessException {
    AuthorizedResult<Set<Role>> roleAuthResult =
      accessControllerInstantiator.get().listRoles(authenticationContext.getPrincipal(), principal);
    AuthorizationUtil.setAuthorizationDataInContext(roleAuthResult.getAuthorizationResponse());
    AuthorizationUtil.throwIfUnauthorized(roleAuthResult.getAuthorizationResponse());
    AuthorizationUtil
      .incrementCheckMetricExtension(
        securityMetricsService.createEntityIdMetricsContext(InstanceId.SELF),
        roleAuthResult.getAuthorizationResponse());
    return roleAuthResult.getResult();
  }

  @Override
  public Set<Role> listAllRoles() throws AccessException {
    AuthorizedResult<Set<Role>> roleAuthResult = accessControllerInstantiator.get()
      .listAllRoles(authenticationContext.getPrincipal());
    AuthorizationUtil.setAuthorizationDataInContext(roleAuthResult.getAuthorizationResponse());
    AuthorizationUtil.throwIfUnauthorized(roleAuthResult.getAuthorizationResponse());
    AuthorizationUtil
      .incrementCheckMetricExtension(
        securityMetricsService.createEntityIdMetricsContext(InstanceId.SELF),
        roleAuthResult.getAuthorizationResponse());
    return roleAuthResult.getResult();
  }

  @Override
  public Set<GrantedPermission> listGrants(Principal principal) throws AccessException {
    AuthorizedResult<Set<GrantedPermission>> grantedPermissionAuthResult =
      accessControllerInstantiator.get().listGrants(authenticationContext.getPrincipal(), principal);
    AuthorizationUtil.setAuthorizationDataInContext(grantedPermissionAuthResult.getAuthorizationResponse());
    AuthorizationUtil.throwIfUnauthorized(grantedPermissionAuthResult.getAuthorizationResponse());
    AuthorizationUtil
      .incrementCheckMetricExtension(securityMetricsService.createEntityIdMetricsContext(InstanceId.SELF),
                                     grantedPermissionAuthResult.getAuthorizationResponse());
    return grantedPermissionAuthResult.getResult();
  }
}
