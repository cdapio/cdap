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
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.auth.Cipher;
import io.cdap.cdap.security.auth.CipherException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAccessEnforcer.class);

  private final SConfiguration sConf;
  private final AccessControllerInstantiator accessControllerInstantiator;
  @Nullable
  private final Principal masterUser;
  private final int logTimeTakenAsWarn;
  private final Cipher cipher;

  @Inject
  DefaultAccessEnforcer(CConfiguration cConf, SConfiguration sConf,
                        AccessControllerInstantiator accessControllerInstantiator,
                        Cipher cipher) {
    super(cConf);
    this.sConf = sConf;
    this.accessControllerInstantiator = accessControllerInstantiator;
    String masterUserName = AuthorizationUtil.getEffectiveMasterUser(cConf);
    this.masterUser = masterUserName == null ? null : new Principal(masterUserName, Principal.PrincipalType.USER);
    this.logTimeTakenAsWarn = cConf.getInt(Constants.Security.Authorization.EXTENSION_OPERATION_TIME_WARN_THRESHOLD);
    this.cipher = cipher;
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions)
    throws AccessException {
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }
    doEnforce(entity, principal, permissions);
  }

  @Override
  public void enforceOnParent(EntityType entityType, EntityId parentId, Principal principal, Permission permission)
    throws AccessException {
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }
    doEnforce(entityType, parentId, principal, permission);
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal)
    throws AccessException {
    if (!isSecurityAuthorizationEnabled()) {
      return entityIds;
    }

    Set<EntityId> visibleEntities = new HashSet<>();
    // filter out entity id which is in system namespace and principal is the master user
    for (EntityId entityId : entityIds) {
      if (isAccessingSystemNSAsMasterUser(entityId, principal) || isEnforcingOnSamePrincipalId(entityId, principal)) {
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

  private void doEnforce(EntityId entity, Principal principal, Set<? extends Permission> permissions)
    throws AccessException {
    // bypass the check when the principal is the master user and the entity is in the system namespace
    if (isAccessingSystemNSAsMasterUser(entity, principal) || isEnforcingOnSamePrincipalId(entity, principal)) {
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

  private void doEnforce(EntityType entityType, EntityId parentId, Principal principal, Permission permission)
    throws AccessException {
    // bypass the check when the principal is the master user and the entity is in the system namespace
    if (isAccessingSystemNSAsMasterUser(parentId, principal) || isEnforcingOnSamePrincipalId(parentId, principal)) {
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

  private Principal getUserPrinciple(Principal principle) throws AccessException {
    if (principle.getCredential() == null ||
      !sConf.getBoolean(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_ENABLED, false)) {
      return principle;
    }

    // When user credential encryption is enabled, credential should be encrypted upon arrival
    // at router and decrypted right here before calling auth extension.
    try {
      String plainCredential = cipher.decryptStringFromBase64(principle.getCredential());

      return new Principal(principle.getName(),
                           principle.getType(),
                           principle.getKerberosPrincipal(),
                           plainCredential);
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
}
