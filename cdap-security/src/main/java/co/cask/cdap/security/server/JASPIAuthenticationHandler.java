/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import co.cask.cdap.common.conf.Constants;
import org.apache.geronimo.components.jaspi.impl.ServerAuthConfigImpl;
import org.apache.geronimo.components.jaspi.impl.ServerAuthContextImpl;
import org.apache.geronimo.components.jaspi.model.AuthModuleType;
import org.apache.geronimo.components.jaspi.model.ServerAuthConfigType;
import org.apache.geronimo.components.jaspi.model.ServerAuthContextType;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.jaspi.JaspiAuthenticator;
import org.eclipse.jetty.security.jaspi.JaspiAuthenticatorFactory;
import org.eclipse.jetty.security.jaspi.ServletCallbackHandler;
import org.eclipse.jetty.security.jaspi.modules.BasicAuthModule;

import java.util.Collections;
import java.util.HashMap;
import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.message.config.ServerAuthConfig;
import javax.security.auth.message.config.ServerAuthContext;
import javax.security.auth.message.module.ServerAuthModule;

/**
 * An Authentication handler that supports JASPI plugins for External Authentication.
 */
public class JASPIAuthenticationHandler extends AbstractAuthenticationHandler {
  private JAASLoginService loginService;
  private IdentityService identityService;

  @Override
  protected LoginService getHandlerLoginService() {
    if (loginService == null) {
      loginService = new JAASLoginService();
      loginService.setLoginModuleName("JASPI");
      loginService.setConfiguration(getLoginModuleConfiguration());
      loginService.setIdentityService(getHandlerIdentityService());
    }
    return loginService;
  }

  @Override
  protected Authenticator getHandlerAuthenticator() {
    JaspiAuthenticatorFactory jaspiAuthenticatorFactory = new JaspiAuthenticatorFactory();
    jaspiAuthenticatorFactory.setLoginService(getHandlerLoginService());

    HashMap<String, ServerAuthContext> serverAuthContextMap = new HashMap<>();
    ServletCallbackHandler callbackHandler = new ServletCallbackHandler(getHandlerLoginService());
    ServerAuthModule authModule = new BasicAuthModule(callbackHandler, "JAASRealm");
    serverAuthContextMap.put("authContextID", new ServerAuthContextImpl(Collections.singletonList(authModule)));

    ServerAuthContextType serverAuthContextType = new ServerAuthContextType("HTTP", "server *",
                                                                            "authContextID",
                                                                            new AuthModuleType<ServerAuthModule>());
    ServerAuthConfigType serverAuthConfigType = new ServerAuthConfigType(serverAuthContextType, true);
    ServerAuthConfig serverAuthConfig = new ServerAuthConfigImpl(serverAuthConfigType, serverAuthContextMap);
    return new JaspiAuthenticator(serverAuthConfig, null, callbackHandler,
                                  new Subject(), true, getHandlerIdentityService());
  }

  @Override
  protected IdentityService getHandlerIdentityService() {
    if (identityService == null) {
      identityService = new DefaultIdentityService();
    }
    return identityService;
  }

  /**
   * Dynamically load the configuration properties set by the user for a JASPI plugin.
   * @return Configuration
   */
  @Override
  protected Configuration getLoginModuleConfiguration() {
    return new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
        return new AppConfigurationEntry[] {
          new AppConfigurationEntry(handlerProps.get(Constants.Security.LOGIN_MODULE_CLASS_NAME),
                                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, new HashMap<>(handlerProps))
        };
      }
    };
  }
}
