package com.continuuity.logging.guice;

import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.handlers.PingHandler;
import com.continuuity.http.HttpHandler;
import com.continuuity.logging.service.LogSaverStatusService;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

/**
 * Module for LogSaverStatusService
 */
public class LogSaverStatusServiceModule extends PrivateModule {

  @Override
  protected void configure() {
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder
      (binder(), HttpHandler.class, Names.named(Constants.LogSaver.LOG_SAVER_STATUS_HANDLER));
    handlerBinder.addBinding().to(PingHandler.class);
    bind(LogSaverStatusService.class).in(Scopes.SINGLETON);
    expose(LogSaverStatusService.class);
  }
}
