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

import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.util.HashSet;
import java.util.Set;

/**
 * Abstract Class that implements common methods for the {@link AccessEnforcer} interface.
 */
public abstract class AbstractAccessEnforcer implements AccessEnforcer {

  private final boolean securityAuthorizationEnabled;

  AbstractAccessEnforcer(CConfiguration cConf) {
    this.securityAuthorizationEnabled = AuthorizationUtil.isSecurityAuthorizationEnabled(cConf);
  }

  protected boolean isSecurityAuthorizationEnabled() {
    return securityAuthorizationEnabled;
  }
}
