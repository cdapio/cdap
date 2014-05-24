package com.continuuity.security.server;

import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.security.guice.SecurityModules;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;

/**
 *
 */
public class AuthenticationGuiceServletContextListener extends GuiceServletContextListener {
  private final Injector injector;

  @Inject
  public AuthenticationGuiceServletContextListener(Injector injector) {
    this.injector = injector;
  }

  @Override
  protected Injector getInjector() {
    return Guice.createInjector(new IOModule(), new ConfigModule(),
                         new DiscoveryRuntimeModule().getSingleNodeModules(),
                         new SecurityModules().getSingleNodeModules());
  }
}
