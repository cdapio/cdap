package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.inject.AbstractModule;
import org.eclipse.jetty.server.Handler;

import java.util.HashMap;

/**
 * Guice module to bind handlers used by RestEasy context listener.
 */
public class SecurityHandlerModule extends AbstractModule {
  private final HashMap<String, Object> handlerMap;
  private final CConfiguration configuration;

  public SecurityHandlerModule(HashMap<String, Object> map, CConfiguration configuration) {
    this.handlerMap = map;
    this.configuration = configuration;
  }

  @Override
  protected void configure() {
    Class<Handler> handlerClass = (Class<Handler>) configuration.getClass(Constants.Security.AUTH_HANDLER_CLASS,
                                                                                                  null, Handler.class);
    bind(handlerClass).toInstance((AbstractAuthenticationHandler)
                                      handlerMap.get(ExternalAuthenticationServer.HandlerType.AUTHENTICATION_HANDLER));
    bind(GrantAccessToken.class).toInstance((GrantAccessToken)
                                      handlerMap.get(ExternalAuthenticationServer.HandlerType.GRANT_TOKEN_HANDLER));
  }
}
