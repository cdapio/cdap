/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.metrics.query.BatchMetricsHandler;
import com.continuuity.metrics.query.MetricsQueryService;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

/**
 * Base guice module for binding metrics query service classes.
 */
abstract class AbstractMetricsQueryModule extends PrivateModule {

  @Override
  protected final void configure() {
    bindMetricsTable();

    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class);
    handlerBinder.addBinding().to(BatchMetricsHandler.class).in(Scopes.SINGLETON);

    bind(MetricsQueryService.class).in(Scopes.SINGLETON);
    expose(MetricsQueryService.class);
  }

  protected abstract void bindMetricsTable();
}
