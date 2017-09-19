/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.impersonation.SecurityUtil;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * An implementation of {@link AuthorizationEnforcer} that runs on the master. It calls the authorizer directly to
 * enforce authorization policies.
 */
@Singleton
public class DefaultAuthorizationEnforcer extends AbstractAuthorizationEnforcer {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAuthorizationEnforcer.class);

  private final AuthorizerInstantiator authorizerInstantiator;
  @Nullable
  private final Principal masterUser;

  @Inject
  DefaultAuthorizationEnforcer(CConfiguration cConf, AuthorizerInstantiator authorizerInstantiator) {
    super(cConf);
    this.authorizerInstantiator = authorizerInstantiator;
    String masterUserName = AuthorizationUtil.getEffectiveMasterUser(cConf);
    this.masterUser = masterUserName == null ? null : new Principal(masterUserName, Principal.PrincipalType.USER);
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Action action) throws Exception {
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }
    doEnforce(entity, principal, Collections.singleton(action));
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal) throws Exception {
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

    Set<? extends EntityId> difference = Sets.difference(entityIds, visibleEntities);
    LOG.trace("Checking visibility of {} for principal {}.", difference, principal);
    Set<? extends EntityId> moreVisibleEntities = authorizerInstantiator.get().isVisible(difference, principal);
    visibleEntities.addAll(moreVisibleEntities);
    LOG.trace("Getting {} as visible entities", visibleEntities);
    return Collections.unmodifiableSet(visibleEntities);
  }

  private void doEnforce(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    // bypass the check when the principal is the master user and the entity is in the system namespace
    if (isAccessingSystemNSAsMasterUser(entity, principal) || isEnforcingOnSamePrincipalId(entity, principal)) {
      return;
    }
    LOG.debug("Enforcing actions {} on {} for principal {}.", actions, entity, principal);
    authorizerInstantiator.get().enforce(entity, principal, actions);
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
