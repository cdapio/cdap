/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.inject.AbstractModule;
import org.eclipse.jetty.server.Handler;

import java.util.HashMap;

/**
 * Guice module to bind handlers used by RestEasy context listener.
 */
public class SecurityHandlerModule extends AbstractModule {
  private final HashMap<String, Object> handlerMap;
  private final CConfiguration configuration;

  public SecurityHandlerModule(HashMap<String, Object> map, CConfiguration configuration) {
    this.handlerMap = map;
    this.configuration = configuration;
  }

  @Override
  protected void configure() {
    Class<Handler> handlerClass = (Class<Handler>) configuration.getClass(Constants.Security.AUTH_HANDLER_CLASS,
                                                                                                  null, Handler.class);
    bind(handlerClass).toInstance((AbstractAuthenticationHandler)
                                      handlerMap.get(ExternalAuthenticationServer.HandlerType.AUTHENTICATION_HANDLER));
    bind(GrantAccessToken.class).toInstance((GrantAccessToken)
                                      handlerMap.get(ExternalAuthenticationServer.HandlerType.GRANT_TOKEN_HANDLER));
  }
}
