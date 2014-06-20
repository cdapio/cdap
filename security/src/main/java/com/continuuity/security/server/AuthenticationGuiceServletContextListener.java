package com.continuuity.security.server;

import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.security.guice.SecurityModules;
import com.google.common.collect.Lists;
import com.google.inject.Module;
import org.eclipse.jetty.server.Handler;
import org.jboss.resteasy.plugins.guice.GuiceResteasyBootstrapServletContextListener;

import java.util.HashMap;
import java.util.List;
import javax.servlet.ServletContext;

/**
 *
 */
public class AuthenticationGuiceServletContextListener extends GuiceResteasyBootstrapServletContextListener {
  private final HashMap<String, Handler> handlerMap;

  public AuthenticationGuiceServletContextListener(HashMap<String, Handler> map) {
    this.handlerMap = map;
  }

  @Override
  protected List<? extends Module> getModules(ServletContext context) {
    return Lists.newArrayList(new IOModule(), new ConfigModule(),
                              new DiscoveryRuntimeModule().getSingleNodeModules(),
                              new SecurityModules().getSingleNodeModules(),
                              new SecurityJerseyModule(handlerMap));
  }

}
