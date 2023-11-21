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
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Security.Encryption;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.metrics.ProgramTypeMetricTag;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.encryption.AeadCipher;
import io.cdap.cdap.security.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.encryption.CipherException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link io.cdap.cdap.security.spi.authorization.AccessEnforcer} that runs on
 * the master. It calls the access controller directly to enforce authorization policies.
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
  private final boolean metricsCollectionEnabled;
  private final boolean metricsTagsEnabled;
  private final MetricsCollectionService metricsCollectionService;
  private final AeadCipher userEncryptionAeadCipher;

  @Inject
  DefaultAccessEnforcer(CConfiguration cConf, SConfiguration sConf,
      AccessControllerInstantiator accessControllerInstantiator,
      @Named(INTERNAL_ACCESS_ENFORCER) AccessEnforcer internalAccessEnforcer,
      MetricsCollectionService metricsCollectionService,
      @Named(UserCredentialAeadEncryptionModule.USER_CREDENTIAL_ENCRYPTION)
          AeadCipher userEncryptionAeadCipher) {
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
    this.metricsCollectionEnabled = cConf.getBoolean(
        Constants.Metrics.AUTHORIZATION_METRICS_ENABLED, false);
    this.metricsTagsEnabled = cConf.getBoolean(Constants.Metrics.AUTHORIZATION_METRICS_TAGS_ENABLED,
        false);
    this.metricsCollectionService = metricsCollectionService;
    this.userEncryptionAeadCipher = userEncryptionAeadCipher;
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions)
      throws AccessException {
    MetricsContext metricsContext = createEntityIdMetricsContext(entity);
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
    if (isAccessingSystemNSAsMasterUser(entity, principal) || isEnforcingOnSamePrincipalId(entity,
        principal)) {
      metricsContext.increment(Constants.Metrics.Authorization.EXTENSION_CHECK_BYPASS_COUNT, 1);
      return;
    }

    principal = getUserPrinciple(principal);

    LOG.trace("Enforcing permissions {} on {} for principal {}.", permissions, entity, principal);
    long startTime = System.nanoTime();
    try {
      accessControllerInstantiator.get().enforce(entity, principal, permissions);
      metricsContext.increment(Constants.Metrics.Authorization.EXTENSION_CHECK_SUCCESS_COUNT, 1);
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
    MetricsContext metricsContext = createEntityIdMetricsContext(parentId);
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
    if (isAccessingSystemNSAsMasterUser(parentId, principal) || isEnforcingOnSamePrincipalId(
        parentId, principal)) {
      metricsContext.increment(Constants.Metrics.Authorization.EXTENSION_CHECK_BYPASS_COUNT, 1);
      return;
    }

    principal = getUserPrinciple(principal);

    LOG.trace("Enforcing permission {} on {} in {} for principal {}.", permission, entityType,
        parentId, principal);
    long startTime = System.nanoTime();
    try {
      accessControllerInstantiator.get()
          .enforceOnParent(entityType, parentId, principal, permission);
      metricsContext.increment(Constants.Metrics.Authorization.EXTENSION_CHECK_SUCCESS_COUNT, 1);
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
    MetricsContext metricsContext = createEntityIdMetricsContext(null);
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
      if (isAccessingSystemNSAsMasterUser(entityId, principal) || isEnforcingOnSamePrincipalId(
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
      moreVisibleEntities = accessControllerInstantiator.get().isVisible(difference, principal);
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


  private boolean isAccessingSystemNSAsMasterUser(EntityId entityId, Principal principal) {
    return entityId instanceof NamespacedEntityId
        && ((NamespacedEntityId) entityId).getNamespaceId().equals(NamespaceId.SYSTEM)
        && principal.equals(masterUser);
  }

  private boolean isEnforcingOnSamePrincipalId(EntityId entityId, Principal principal) {
    return entityId.getEntityType().equals(EntityType.KERBEROSPRINCIPAL)
        && principal.getName().equals(entityId.getEntityName());
  }

  /**
   * Constructs metrics tags for a given entity ID.
   */
  static Map<String, String> createEntityIdMetricsTags(EntityId entityId) {
    Map<String, String> tags = new HashMap<>();
    for (EntityId currEntityId : entityId.getHierarchy()) {
      addTagsForEntityId(tags, currEntityId);
    }
    return tags;
  }

  private static void addTagsForEntityId(Map<String, String> tags, EntityId entityId) {
    switch (entityId.getEntityType()) {
      case INSTANCE:
        tags.put(Constants.Metrics.Tag.INSTANCE_ID, entityId.getEntityName());
        break;
      case NAMESPACE:
        tags.put(Constants.Metrics.Tag.NAMESPACE, entityId.getEntityName());
        break;
      case PROGRAM_RUN:
        ProgramRunId programRunId = (ProgramRunId) entityId;
        tags.put(Constants.Metrics.Tag.RUN_ID, entityId.getEntityName());
        tags.put(ProgramTypeMetricTag.getTagName(programRunId.getType()),
            programRunId.getProgram());
        break;
      case DATASET:
        tags.put(Constants.Metrics.Tag.DATASET, entityId.getEntityName());
        break;
      case APPLICATION:
        tags.put(Constants.Metrics.Tag.APP, entityId.getEntityName());
        break;
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        tags.put(Constants.Metrics.Tag.PROGRAM, programId.getProgram());
        tags.put(Constants.Metrics.Tag.PROGRAM_TYPE,
            ProgramTypeMetricTag.getTagName(programId.getType()));
        break;
      case PROFILE:
        tags.put(Constants.Metrics.Tag.PROFILE, entityId.getEntityName());
        break;
      case OPERATION_RUN:
        tags.put(Constants.Metrics.Tag.OPERATION_RUN, entityId.getEntityName());
        break;
      default:
        // No tags to set
    }
  }

  /**
   * Constructs tags and returns a metrics context for a given entity ID.
   */
  private MetricsContext createEntityIdMetricsContext(EntityId entityId) {
    if (!metricsCollectionEnabled) {
      return new NoopMetricsContext();
    }
    Map<String, String> tags = Collections.emptyMap();
    if (metricsTagsEnabled && entityId != null) {
      tags = createEntityIdMetricsTags(entityId);
    }
    return metricsCollectionService == null ? new NoopMetricsContext(tags)
        : metricsCollectionService.getContext(tags);
  }
}
