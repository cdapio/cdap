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

package co.cask.cdap.security.server;

import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;

import javax.security.auth.login.Configuration;
/**
 * Handler for knox authentication of users.
 */
public class KnoxAuthenticationHandler extends AbstractAuthenticationHandler {
  private IdentityService identityService;

  @Override
  protected LoginService getHandlerLoginService() {
    KnoxJAASLoginService knoxLoginService = new KnoxJAASLoginService();
    knoxLoginService.setConfiguration(getLoginModuleConfiguration());
    return knoxLoginService;
  }

  @Override
  protected Authenticator getHandlerAuthenticator() {
    return new KnoxBasicAuthenticator(handlerProps);
  }

  @Override
  protected IdentityService getHandlerIdentityService() {
    if (identityService == null) {
      identityService = new DefaultIdentityService();
    }
    return identityService;
  }

  @Override
  protected Configuration getLoginModuleConfiguration() {
    return null;
  }
}
