package com.continuuity.security.server;

import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

/**
 *
 */
public class SecurityHandlerModule extends JerseyServletModule {
  @Override
  protected void configureServlets() {
    bindings();
    filters();
  }

  private void bindings() {
    bind(BasicAuthenticationHandler.class);

  }
  private void filters() {
    filter("/v1/*").through(GuiceContainer.class);
  }
}
