package com.continuuity.logging.guice;

import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.handlers.PingHandler;
import com.continuuity.http.HttpHandler;
import com.continuuity.logging.service.LogSaverService;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

/**
 * Module for LogSaverService
 */
public class LogSaverServiceModule extends PrivateModule {

  @Override
  protected void configure() {
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class,
                                                                      Names.named(
                                                                        Constants.LogSaver.LOG_SAVER_HANDLER));
    handlerBinder.addBinding().to(PingHandler.class);
    bind(LogSaverService.class).in(Scopes.SINGLETON);
    expose(LogSaverService.class);
  }
}
