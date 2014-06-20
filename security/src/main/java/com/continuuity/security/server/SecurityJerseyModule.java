package com.continuuity.security.server;

import com.google.inject.AbstractModule;
import org.eclipse.jetty.server.Handler;

import java.util.HashMap;

/**
 *
 */
public class SecurityJerseyModule extends AbstractModule {
  private final HashMap<String, Handler> handlerMap;

  public SecurityJerseyModule(HashMap<String, Handler> map) {
    this.handlerMap = map;
  }

  @Override
  protected void configure() {
    bind(LDAPAuthenticationHandler.class).toInstance((LDAPAuthenticationHandler)
                                      handlerMap.get(ExternalAuthenticationServer.HandlerType.AUTHENTICATION_HANDLER));
    bind(GrantAccessTokenHandler.class).toInstance((GrantAccessTokenHandler)
                                      handlerMap.get(ExternalAuthenticationServer.HandlerType.GRANT_TOKEN_HANDLER));
  }
}
