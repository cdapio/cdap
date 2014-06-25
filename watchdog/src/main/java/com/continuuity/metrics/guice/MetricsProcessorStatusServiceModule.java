package com.continuuity.metrics.guice;

import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.handlers.PingHandler;
import com.continuuity.http.HttpHandler;
import com.continuuity.metrics.process.MetricsProcessorStatusService;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

/**
 * Metrics Processor Service Module for the MetricsProcessorStatusService
 */
public class MetricsProcessorStatusServiceModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(MetricsProcessorStatusService.class).in(Scopes.SINGLETON);
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder
      (binder(), HttpHandler.class, Names.named(Constants.MetricsProcessor.METRICS_PROCESSOR_STATUS_HANDLER));
    handlerBinder.addBinding().to(PingHandler.class);
    expose(MetricsProcessorStatusService.class);
  }
}
