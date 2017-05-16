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
import co.cask.cdap.proto.id.ParentedId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

/**
 * An implementation of {@link AuthorizationEnforcer} that runs on the master. It calls the authorizer directly to
 * enforce authorization policies.
 */
@Singleton
public class DefaultAuthorizationEnforcer extends AbstractAuthorizationEnforcer {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAuthorizationEnforcer.class);

  private final AuthorizerInstantiator authorizerInstantiator;
  private final boolean propagatePrivileges;

  @Inject
  DefaultAuthorizationEnforcer(CConfiguration cConf, AuthorizerInstantiator authorizerInstantiator) {
    super(cConf);
    this.propagatePrivileges = cConf.getBoolean(Constants.Security.Authorization.PROPAGATE_PRIVILEGES);
    this.authorizerInstantiator = authorizerInstantiator;
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Action action) throws Exception {
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }
    LOG.debug("Enforcing actions {} on {} for principal {}.", action, entity, principal);
    doEnforce(entity, principal, Collections.singleton(action), true);
  }

  private boolean doEnforce(EntityId entity, Principal principal,
                            Set<Action> actions, boolean exceptionOnFailure) throws Exception {
    // If privilege propagation is enabled then check for the privilege on the parent, if any.
    if (isPrivilegePropagationEnabled()) {
      if (entity instanceof ParentedId) {
        LOG.debug("Privilege propagation is enabled. Checking privilege for the parent of {}", entity.getEntityName());
        if (doEnforce(((ParentedId) entity).getParent(), principal, actions, false)) {
          return true;
        }
      }
    }
    try {
      authorizerInstantiator.get().enforce(entity, principal, actions);
      LOG.debug("Enforcing actions {} on {} for principal {}. Succeeded", actions, entity, principal);
    } catch (Exception e) {
      if (exceptionOnFailure) {
        throw new UnauthorizedException(principal, actions, entity);
      } else {
        return false;
      }
    }
    return true;
  }

  private boolean isPrivilegePropagationEnabled() {
    return propagatePrivileges;
  }
}
