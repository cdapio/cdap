package com.continuuity.security.server;

import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.security.guice.SecurityModules;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.eclipse.jetty.server.Handler;

/**
 *
 */
public class AuthenticationGuiceServletContextListener extends GuiceServletContextListener {
  private BasicAuthenticationHandler authenticationHandler;
  private GrantAccessTokenHandler grantAccessTokenHandler;

  public AuthenticationGuiceServletContextListener(Handler authenticationHandler,
                                                   Handler grantAccessTokenHandler) {
    this.authenticationHandler = (BasicAuthenticationHandler) authenticationHandler;
    this.grantAccessTokenHandler = (GrantAccessTokenHandler) grantAccessTokenHandler;
  }

  @Override
  protected Injector getInjector() {
    return Guice.createInjector(new IOModule(), new ConfigModule(),
                                new DiscoveryRuntimeModule().getSingleNodeModules(),
                                new SecurityModules().getSingleNodeModules(),
                                new JerseyServletModule() {
                                  @Override
                                  protected void configureServlets() {
                                    bind(BasicAuthenticationHandler.class).toInstance(authenticationHandler);
                                    bind(GrantAccessTokenHandler.class).toInstance(grantAccessTokenHandler);
                                    filter("/*").through(GuiceContainer.class);
                                  }
                                }
                         );
  }

}
