/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.ParentedId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

/**
 * Default implementation of {@link AuthorizationEnforcementService}.
 */
@Singleton
public class DefaultAuthorizationEnforcer implements AuthorizationEnforcer {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAuthorizationEnforcer.class);
  final AuthorizerInstantiator authorizerInstantiator;

  private static final Predicate<EntityId> ALLOW_ALL = new Predicate<EntityId>() {
    @Override
    public boolean apply(EntityId entityId) {
      return true;
    }
  };
  private final boolean securityEnabled;
  private final boolean authorizationEnabled;
  private final boolean propagatePrivileges;

  @Inject
  DefaultAuthorizationEnforcer(CConfiguration cConf, AuthorizerInstantiator authorizerInstantiator) {
    this.securityEnabled = cConf.getBoolean(Constants.Security.ENABLED);
    this.authorizationEnabled = cConf.getBoolean(Constants.Security.Authorization.ENABLED);
    this.propagatePrivileges = cConf.getBoolean(Constants.Security.Authorization.PROPAGATE_PRIVILEGES);
    this.authorizerInstantiator = authorizerInstantiator;
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Action action) throws Exception {
    enforce(entity, principal, Collections.singleton(action));
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }

    doEnforce(entity, principal, actions, true);
  }


  @Override
  public Predicate<EntityId> createFilter(final Principal principal) throws Exception {
    return new Predicate<EntityId>() {
      @Override
      public boolean apply(EntityId entityId) {
        System.out.println("Filtering for " + entityId.getEntityName());
        for (Action action : Action.values()) {
          try {
            enforce(entityId, principal, action);
            return true;
          } catch (Exception ignored) {
            System.out.println("Don't have " + action.name() + " on " + entityId.getEntityName());
          }
        }
        return false;
      }
    };
  }

//  @Override
//  public Predicate<EntityId> createFilter(Principal principal) throws Exception {
//    if (!isSecurityAuthorizationEnabled()) {
//      return ALLOW_ALL;
//    }
//
//    return authorizer.createFilter(principal);
//  }


  private boolean doEnforce(EntityId entity, Principal principal,
                            Set<Action> actions, boolean exceptionOnFailure) throws Exception {
    if (isPrivilegePropagationEnabled()) {
      if (entity instanceof ParentedId) {
        System.out.println("Checking parent for " + entity.getEntityName());
        if (doEnforce(((ParentedId) entity).getParent(), principal, actions, false)) {
          return true;
        }
      }
    }
    LOG.trace("Enforcing actions {} on {} for principal {}.", actions, entity, principal);
    try {
      authorizerInstantiator.get().enforce(entity, principal, actions);
    } catch (Exception e) {
      if (exceptionOnFailure) {
        throw new UnauthorizedException(principal, actions, entity);
      } else {
        return false;
      }
    }

    return true;
  }

  private boolean isSecurityAuthorizationEnabled() {
    return securityEnabled && authorizationEnabled;
  }

  private boolean isPrivilegePropagationEnabled() {
    return propagatePrivileges;
  }
}
