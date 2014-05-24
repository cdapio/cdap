package com.continuuity.security.server;

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
    return injector;
  }
}
