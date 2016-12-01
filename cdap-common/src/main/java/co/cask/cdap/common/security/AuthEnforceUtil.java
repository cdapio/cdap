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

package co.cask.cdap.common.security;

import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;

import java.util.Set;

/**
 * Class used by {@link AuthEnforceRewriter} to rewrite classes with {@link AuthEnforce} annotation and call
 * enforcement methods in this class to perform authorization enforcement.
 */
// Note: Do no remove the public modifier of this class. This class is marked public even though its only usage is
// from the package because after class rewrite AuthEnforce annotations are rewritten to make call to the methods
// in this class and since AuthEnforce annotation can be in any package after class rewrite this class methods
// might be being called from other packages.
public final class AuthEnforceUtil {

  private AuthEnforceUtil() {
    // no-op
  }

  /**
   * Performs authorization enforcement
   *
   * @param authorizationEnforcer the {@link AuthorizationEnforcer} to use for performing the enforcement
   * @param entities the {@link EntityId} on which authorization is to be enforced
   * @param authenticationContext the {@link AuthenticationContext}  of the user that performs the action
   * @param actions the {@link Action}s being performed
   * @throws Exception
   */
  public static void enforce(AuthorizationEnforcer authorizationEnforcer, EntityId entities,
                             AuthenticationContext authenticationContext, Set<Action> actions) throws Exception {
    authorizationEnforcer.enforce(entities, authenticationContext.getPrincipal(), actions);
  }
}
