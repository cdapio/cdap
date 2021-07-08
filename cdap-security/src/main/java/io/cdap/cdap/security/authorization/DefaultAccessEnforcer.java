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
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.auth.CipherException;
import io.cdap.cdap.security.auth.TinkCipher;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * An implementation of {@link io.cdap.cdap.security.spi.authorization.AccessEnforcer} that runs on the master.
 * It calls the access controller directly to enforce authorization policies.
 */
@Singleton
public class DefaultAccessEnforcer extends AbstractAccessEnforcer {
  public static final String INTERNAL_ACCESS_ENFORCER = "internal";

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAccessEnforcer.class);

  private final SConfiguration sConf;
  private final AccessControllerInstantiator accessControllerInstantiator;
  @Nullable
  private final Principal masterUser;
  private final int logTimeTakenAsWarn;
  private final AccessEnforcer internalAccessEnforcer;
  private final boolean internalAuthEnabled;

  @Inject
  DefaultAccessEnforcer(CConfiguration cConf, SConfiguration sConf,
                        AccessControllerInstantiator accessControllerInstantiator,
                        @Named(INTERNAL_ACCESS_ENFORCER) AccessEnforcer internalAccessEnforcer) {
    super(cConf);
    this.sConf = sConf;
    this.accessControllerInstantiator = accessControllerInstantiator;
    String masterUserName = AuthorizationUtil.getEffectiveMasterUser(cConf);
    this.masterUser = masterUserName == null ? null : new Principal(masterUserName, Principal.PrincipalType.USER);
    this.logTimeTakenAsWarn = cConf.getInt(Constants.Security.Authorization.EXTENSION_OPERATION_TIME_WARN_THRESHOLD);
    this.internalAccessEnforcer = internalAccessEnforcer;
    this.internalAuthEnabled = SecurityUtil.isInternalAuthEnabled(cConf);
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions)
    throws AccessException {
    if (internalAuthEnabled && principal.getFullCredential() != null && principal.getFullCredential().getType()
      == Credential.CredentialType.INTERNAL) {
      LOG.trace("Internal Principal enforce({}, {}, {})", entity, principal, permissions);
      internalAccessEnforcer.enforce(entity, principal, permissions);
      return;
    }
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }
    // bypass the check when the principal is the master user and the entity is in the system namespace
    if (isAccessingSystemNSAsMasterUser(entity, principal) || isEnforcingOnSamePrincipalId(entity, principal)
      || isRootUGIPrincipalHack(principal)) {
      return;
    }

    principal = getUserPrinciple(principal);

    LOG.trace("Enforcing permissions {} on {} for principal {}.", permissions, entity, principal);
    long startTime = System.nanoTime();
    try {
      accessControllerInstantiator.get().enforce(entity, principal, permissions);
    } finally {
      long timeTaken = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
      String logLine = "Enforced permissions {} on {} for principal {}. Time spent in enforcement was {} ms.";
      if (timeTaken > logTimeTakenAsWarn) {
        LOG.warn(logLine, permissions, entity, principal, timeTaken);
      } else {
        LOG.trace(logLine, permissions, entity, principal, timeTaken);
      }
    }
  }

  @Override
  public void enforceOnParent(EntityType entityType, EntityId parentId, Principal principal, Permission permission)
    throws AccessException {
    if (internalAuthEnabled && principal.getFullCredential() != null && principal.getFullCredential().getType()
      == Credential.CredentialType.INTERNAL) {
      LOG.trace("Internal Principal enforceOnParent({}, {}, {})", parentId, principal, permission);
      internalAccessEnforcer.enforceOnParent(entityType, parentId, principal, permission);
      return;
    }

    if (!isSecurityAuthorizationEnabled()) {
      return;
    }

    // bypass the check when the principal is the master user and the entity is in the system namespace
    if (isAccessingSystemNSAsMasterUser(parentId, principal) || isEnforcingOnSamePrincipalId(parentId, principal)
      || isRootUGIPrincipalHack(principal)) {
      return;
    }

    principal = getUserPrinciple(principal);

    LOG.trace("Enforcing permission {} on {} in {} for principal {}.", permission, entityType, parentId, principal);
    long startTime = System.nanoTime();
    try {
      accessControllerInstantiator.get().enforceOnParent(entityType, parentId, principal, permission);
    } finally {
      long timeTaken = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
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
    if (internalAuthEnabled && principal.getFullCredential() != null && principal.getFullCredential().getType()
      == Credential.CredentialType.INTERNAL) {
      LOG.trace("Internal Principal enforce({}, {})", entityIds, principal);
      return internalAccessEnforcer.isVisible(entityIds, principal);
    }

    if (!isSecurityAuthorizationEnabled()) {
      return entityIds;
    }

    Set<EntityId> visibleEntities = new HashSet<>();
    // filter out entity id which is in system namespace and principal is the master user
    for (EntityId entityId : entityIds) {
      if (isAccessingSystemNSAsMasterUser(entityId, principal) || isEnforcingOnSamePrincipalId(entityId, principal)
        || isRootUGIPrincipalHack(principal)) {
        visibleEntities.add(entityId);
      }
    }

    principal = getUserPrinciple(principal);

    Set<? extends EntityId> difference = Sets.difference(entityIds, visibleEntities);
    LOG.trace("Checking visibility of {} for principal {}.", difference, principal);
    Set<? extends EntityId> moreVisibleEntities;
    long startTime = System.nanoTime();
    try {
      moreVisibleEntities = accessControllerInstantiator.get().isVisible(difference, principal);
    } finally {
      long timeTaken = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
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
    if (principal.getFullCredential() == null ||
      !sConf.getBoolean(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_ENABLED, false)) {
      return principal;
    }

    Credential userCredential = principal.getFullCredential();
    if (userCredential == null || !userCredential.getType().equals(Credential.CredentialType.EXTERNAL_ENCRYPTED)) {
      return principal;
    }

    // When user credential encryption is enabled, credential should be encrypted upon arrival
    // at router and decrypted right here before calling auth extension.
    try {
      String plainCredential = new String(new TinkCipher(sConf).decryptFromBase64(userCredential.getValue(),
                                                                                  null),
                                          StandardCharsets.UTF_8);
      return new Principal(principal.getName(),
                           principal.getType(),
                           principal.getKerberosPrincipal(),
                           new Credential(plainCredential, Credential.CredentialType.EXTERNAL));
    } catch (CipherException e) {
      throw new AccessException("Failed to decrypt credential in principle: " + e.getMessage(), e);
    }
  }


  private boolean isAccessingSystemNSAsMasterUser(EntityId entityId, Principal principal) {
    return entityId instanceof NamespacedEntityId &&
      ((NamespacedEntityId) entityId).getNamespaceId().equals(NamespaceId.SYSTEM) && principal.equals(masterUser);
  }

  private boolean isEnforcingOnSamePrincipalId(EntityId entityId, Principal principal) {
    return entityId.getEntityType().equals(EntityType.KERBEROSPRINCIPAL) &&
      principal.getName().equals(entityId.getEntityName());
  }

  // HACK workaround for CDAP-18172
  private boolean isRootUGIPrincipalHack(Principal principal) {
    // We assume that the principal UGI for the system service apps are "root"
    return principal != null && principal.getName() != null && principal.getName().equals("root");
  }
}
