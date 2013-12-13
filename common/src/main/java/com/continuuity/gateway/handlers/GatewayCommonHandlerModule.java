package com.continuuity.gateway.handlers;

import com.continuuity.common.http.core.HttpHandler;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

/**
 * Guice module for handlers defined in Gateway.
 */
public class GatewayCommonHandlerModule extends AbstractModule {
  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(PingHandler.class).in(Scopes.SINGLETON);
        expose(PingHandler.class);
      }
    });

    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class);
    handlerBinder.addBinding().to(PingHandler.class);
  }
}
