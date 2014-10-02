/*
 * Copyright © 2014 Cask Data, Inc.
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

import javax.inject.Inject;

/**
 * Wraps objects that require authorization with authorization.
 */
public abstract class AuthorizationProxyFactory {

  protected final CConfiguration cConf;
  protected final AuthorizationClient authorizationClient;

  @Inject
  public AuthorizationProxyFactory(CConfiguration cConf, AuthorizationClient authorizationClient) {
    this.cConf = cConf;
    this.authorizationClient = authorizationClient;
  }

  protected abstract <T> T doWrap(T object);

  public <T> T wrap(T object) {
    if (cConf.getBoolean(Constants.Security.CFG_SECURITY_ENABLED, false)) {
      return doWrap(object);
    } else {
      return object;
    }
  }
}
