package com.continuuity.logging.gateway.handlers;

import com.continuuity.common.http.core.HttpHandler;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

/**
 * Guice module for handlers defined for logging.
 */
public class LogHandlerModule extends AbstractModule {
  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(LogHandler.class).in(Scopes.SINGLETON);
        expose(LogHandler.class);
      }
    });

    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class);
    handlerBinder.addBinding().to(LogHandler.class);
  }
}
