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
import com.google.common.collect.Lists;
import com.google.inject.Module;
import org.jboss.resteasy.plugins.guice.GuiceResteasyBootstrapServletContextListener;

import java.util.HashMap;
import java.util.List;
import javax.servlet.ServletContext;

/**
 * RestEasy context listener used to bind handlers. Enables usage of JAX-RS annotations.
 */
public class AuthenticationGuiceServletContextListener extends GuiceResteasyBootstrapServletContextListener {
  private final HashMap<String, Object> handlerMap;
  private final CConfiguration configuration;

  /**
   * Create an AuthenticationGuiceServletContextListener that binds handlers.
   * @param map
   * @param configuration
   */
  public AuthenticationGuiceServletContextListener(HashMap<String, Object> map, CConfiguration configuration) {
    this.handlerMap = map;
    this.configuration = configuration;
  }

  @Override
  protected List<? extends Module> getModules(ServletContext context) {
    return Lists.newArrayList(new SecurityHandlerModule(handlerMap, configuration));
  }

}
