package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
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
