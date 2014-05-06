package com.continuuity.gateway.handlers;

import com.continuuity.http.HttpHandler;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

/**
 * Guice module for handlers defined in Gateway.
 */
public class GatewayCommonHandlerModule extends AbstractModule {
  @Override
  protected void configure() {
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class);
    handlerBinder.addBinding().to(PingHandler.class);
  }
}
