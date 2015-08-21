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

import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import org.jboss.resteasy.plugins.guice.GuiceResteasyBootstrapServletContextListener;

import java.util.List;
import java.util.Map;
import javax.servlet.ServletContext;

/**
 * RestEasy context listener used to bind handlers. Enables usage of JAX-RS annotations.
 */
public class AuthenticationGuiceServletContextListener extends GuiceResteasyBootstrapServletContextListener {
  private final Map<String, Object> handlerMap;

  /**
   * Create an AuthenticationGuiceServletContextListener that binds handlers.
   * @param handlerMap map of handlers.
   */
  public AuthenticationGuiceServletContextListener(Map<String, Object> handlerMap) {
    this.handlerMap = handlerMap;
  }

  @Override
  protected List<? extends Module> getModules(ServletContext context) {
    return Lists.newArrayList(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(GrantAccessToken.class).toInstance(
            (GrantAccessToken) handlerMap.get(ExternalAuthenticationServer.HandlerType.GRANT_TOKEN_HANDLER)
          );
        }
      });
  }
}
