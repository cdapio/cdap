package com.continuuity.metrics.guice;

import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.handlers.PingHandler;
import com.continuuity.http.HttpHandler;
import com.continuuity.metrics.process.MetricsProcessorService;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

/**
 * Metrics Processort Service Module for service discovery
 */
public class MetricsProcessorServiceModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(MetricsProcessorService.class).in(Scopes.SINGLETON);
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder
      (binder(), HttpHandler.class, Names.named(Constants.MetricsProcessor.METRICS_PROCESSOR_HANDLER));
    handlerBinder.addBinding().to(PingHandler.class);
    expose(MetricsProcessorService.class);
  }
}
