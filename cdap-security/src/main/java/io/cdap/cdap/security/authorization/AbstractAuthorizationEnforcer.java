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

package io.cdap.cdap.security.authorization;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.util.EnumSet;
import java.util.Set;

/**
 * Abstract Class that implements common methods for the {@link AuthorizationEnforcer} interface.
 */
public abstract class AbstractAuthorizationEnforcer implements AuthorizationEnforcer {

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

  protected boolean isSecurityAuthorizationEnabled() {
    return securityAuthorizationEnabled;
  }
}
