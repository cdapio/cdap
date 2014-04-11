package com.continuuity.metrics.guice;

import com.continuuity.common.conf.Constants;
import com.continuuity.http.HttpHandler;
import com.continuuity.metrics.data.DefaultMetricsTableFactory;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.metrics.query.BatchMetricsHandler;
import com.continuuity.metrics.query.DeleteMetricsHandler;
import com.continuuity.metrics.query.MetricsDiscoveryHandler;
import com.continuuity.metrics.query.MetricsQueryHandler;
import com.continuuity.metrics.query.MetricsService;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

/**
 * Metrics http handlers.
 */
public class MetricsHandlerModule extends AbstractModule {
  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(MetricsTableFactory.class).to(DefaultMetricsTableFactory.class).in(Scopes.SINGLETON);

        bind(MetricsService.class);
        bind(BatchMetricsHandler.class).in(Scopes.SINGLETON);
        bind(DeleteMetricsHandler.class).in(Scopes.SINGLETON);
        bind(MetricsDiscoveryHandler.class).in(Scopes.SINGLETON);
        bind(MetricsQueryHandler.class).in(Scopes.SINGLETON);

        expose(MetricsService.class);
        expose(BatchMetricsHandler.class);
        expose(DeleteMetricsHandler.class);
        expose(MetricsDiscoveryHandler.class);
        expose(MetricsQueryHandler.class);
      }
    });

    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class,
                                                                      Names.named(Constants.Service.METRICS));
    handlerBinder.addBinding().to(BatchMetricsHandler.class);
    handlerBinder.addBinding().to(DeleteMetricsHandler.class);
    handlerBinder.addBinding().to(MetricsDiscoveryHandler.class);
    handlerBinder.addBinding().to(MetricsQueryHandler.class);
  }
}
