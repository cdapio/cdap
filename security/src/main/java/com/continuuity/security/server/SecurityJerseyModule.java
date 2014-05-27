package com.continuuity.security.server;

import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.eclipse.jetty.server.Handler;

import java.util.HashMap;

/**
 *
 */
public class SecurityJerseyModule extends JerseyServletModule {
  private final HashMap<String, Handler> handlerMap;

  public SecurityJerseyModule(HashMap<String, Handler> map) {
    this.handlerMap = map;
  }

  @Override
  protected void configureServlets() {
    bind(LDAPAuthenticationHandler.class).toInstance((LDAPAuthenticationHandler)
                                      handlerMap.get(ExternalAuthenticationServer.HandlerType.AUTHENTICATION_HANDLER));
    bind(GrantAccessTokenHandler.class).toInstance((GrantAccessTokenHandler)
                                      handlerMap.get(ExternalAuthenticationServer.HandlerType.GRANT_TOKEN_HANDLER));
    filter("/*").through(GuiceContainer.class);
  }
}
