/*
 * Copyright © 2017 Cask Data, Inc.
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
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

/**
 * Abstract Class that implements common methods for the {@link AuthorizationEnforcer} interface.
 */
public abstract class AbstractAuthorizationEnforcer implements AuthorizationEnforcer {

  private static final Predicate<EntityId> ALLOW_ALL = new Predicate<EntityId>() {
    @Override
    public boolean apply(EntityId entityId) {
      return true;
    }
  };

  private final boolean securityAuthorizationEnabled;

  AbstractAuthorizationEnforcer(CConfiguration cConf) {
    this.securityAuthorizationEnabled = AuthorizationUtil.isSecurityAuthorizationEnabled(cConf);
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }

    Set<Action> disallowed = EnumSet.noneOf(Action.class);
    UnauthorizedException unauthorizedException = new UnauthorizedException(principal, entity);
    for (Action action : actions) {
      try {
        enforce(entity, principal, action);
      } catch (UnauthorizedException e) {
        disallowed.add(action);
        unauthorizedException.addSuppressed(e);
      }
    }
    if (!disallowed.isEmpty()) {
      throw new UnauthorizedException(principal, disallowed, entity, unauthorizedException);
    }
  }

  @Override
  public Predicate<EntityId> createFilter(final Principal principal) throws Exception {
    if (!isSecurityAuthorizationEnabled()) {
      return ALLOW_ALL;
    }
    return new Predicate<EntityId>() {
      @Override
      public boolean apply(EntityId entityId) {
        for (Action action : Action.values()) {
          try {
            enforce(entityId, principal, action);
            return true;
          } catch (UnauthorizedException ignored) {
            // The principal does not have this particular privilege but for filter as long as they have any privilege
            // we return true, so ignoring this exception.
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }
        return false;
      }
    };
  }

  protected boolean isSecurityAuthorizationEnabled() {
    return securityAuthorizationEnabled;
  }
}
