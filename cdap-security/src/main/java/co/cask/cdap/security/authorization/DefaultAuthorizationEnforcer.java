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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ParentedId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.impersonation.SecurityUtil;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
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
  private final boolean propagatePrivileges;
  @Nullable
  private final Principal masterUser;

  @Inject
  DefaultAuthorizationEnforcer(CConfiguration cConf, AuthorizerInstantiator authorizerInstantiator) {
    super(cConf);
    this.propagatePrivileges = cConf.getBoolean(Constants.Security.Authorization.PROPAGATE_PRIVILEGES);
    this.authorizerInstantiator = authorizerInstantiator;
    if (isSecurityAuthorizationEnabled()) {
      String masterPrincipal = SecurityUtil.getMasterPrincipal(cConf);
      if (masterPrincipal == null) {
        throw new RuntimeException("Kerberos master principal is null. Authorization can only be used when kerberos" +
                                     " is used");
      }
      try {
        this.masterUser = new Principal(new KerberosName(masterPrincipal).getShortName(), Principal.PrincipalType.USER);
      } catch (IOException e) {
        throw new RuntimeException(String.format("Failed to translate the principal name %s to an operating system " +
                                                   "user name.", masterPrincipal), e);
      }
    } else {
      masterUser = null;
    }
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Action action) throws Exception {
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }
    doEnforce(entity, principal, Collections.singleton(action));
  }

  private void doEnforce(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    // bypass the check when the principal is the master user and the entity is in the system namespace
    if (entity instanceof NamespacedEntityId &&
      ((NamespacedEntityId) entity).getNamespaceId().equals(NamespaceId.SYSTEM) &&
      principal.equals(masterUser)) {
      return;
    }
    LOG.debug("Enforcing actions {} on {} for principal {}.", actions, entity, principal);
    try {
      authorizerInstantiator.get().enforce(entity, principal, actions);
    } catch (UnauthorizedException e) {
      // If privilege propagation is enabled then check for the privilege on the parent, if any.
      if (propagatePrivileges && entity instanceof ParentedId) {
        LOG.trace("Checking privilege for the parent of {}", entity.getEntityName());
        doEnforce(((ParentedId) entity).getParent(), principal, actions);
      } else {
        throw e;
      }
    }
  }
}
