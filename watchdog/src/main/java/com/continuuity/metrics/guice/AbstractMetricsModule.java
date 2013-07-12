/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.metrics.collect.MetricsCollectionService;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.metrics.query.BatchMetricsHandler;
import com.continuuity.metrics.query.MetricsQueryService;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

/**
 * Base guice module for creating different MetricsModule.
 */
abstract class AbstractMetricsModule extends PrivateModule {

  @Override
  protected final void configure() {
    bindTableHandle();

    bind(TransactionOracle.class).to(NoopTransactionOracle.class).in(Scopes.SINGLETON);
    bind(MetricsTableFactory.class).to(DefaultMetricsTableFactory.class).in(Scopes.SINGLETON);

    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class);
    handlerBinder.addBinding().to(BatchMetricsHandler.class).in(Scopes.SINGLETON);

    bind(MetricsQueryService.class).in(Scopes.SINGLETON);
    expose(MetricsQueryService.class);

    expose(MetricsCollectionService.class);
  }

  protected abstract void bindTableHandle();
}
