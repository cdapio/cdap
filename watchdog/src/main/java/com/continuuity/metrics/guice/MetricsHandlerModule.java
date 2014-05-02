package com.continuuity.metrics.guice;

import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.handlers.PingHandler;
import com.continuuity.http.HttpHandler;
import com.continuuity.logging.gateway.handlers.LogHandler;
import com.continuuity.metrics.data.DefaultMetricsTableFactory;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.metrics.query.BatchMetricsHandler;
import com.continuuity.metrics.query.DeleteMetricsHandler;
import com.continuuity.metrics.query.MetricsDiscoveryHandler;
import com.continuuity.metrics.query.MetricsQueryHandler;
import com.continuuity.metrics.query.MetricsQueryService;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

/**
 * Metrics http handlers.
 */
public class MetricsHandlerModule extends PrivateModule {
  @Override
  protected void configure() {
    bind(MetricsTableFactory.class).to(DefaultMetricsTableFactory.class).in(Scopes.SINGLETON);

    bind(MetricsQueryService.class).in(Scopes.SINGLETON);
    expose(MetricsQueryService.class);

    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class,
                                                                      Names.named(Constants.Service.METRICS));
    handlerBinder.addBinding().to(BatchMetricsHandler.class);
    handlerBinder.addBinding().to(DeleteMetricsHandler.class);
    handlerBinder.addBinding().to(MetricsDiscoveryHandler.class);
    handlerBinder.addBinding().to(MetricsQueryHandler.class);
    handlerBinder.addBinding().to(LogHandler.class);
    handlerBinder.addBinding().to(PingHandler.class);
  }
}
