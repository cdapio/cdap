package com.continuuity.security.server;

import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.security.guice.SecurityModules;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import org.eclipse.jetty.server.Handler;

import java.util.HashMap;

/**
 *
 */
public class AuthenticationGuiceServletContextListener extends GuiceServletContextListener {
  private final HashMap<String, Handler> handlerMap;

  public AuthenticationGuiceServletContextListener(HashMap<String, Handler> map) {
    this.handlerMap = map;
  }

  @Override
  protected Injector getInjector() {
    return Guice.createInjector(new IOModule(), new ConfigModule(),
                                new DiscoveryRuntimeModule().getSingleNodeModules(),
                                new SecurityModules().getSingleNodeModules(),
                                new SecurityJerseyModule(handlerMap));
  }

}
