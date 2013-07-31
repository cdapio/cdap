/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.common.utils.Networks;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.query.BatchMetricsHandler;
import com.continuuity.metrics.query.DeleteMetricsHandler;
import com.continuuity.metrics.query.MetricsQueryService;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Base guice module for binding metrics query service classes.
 */
public abstract class AbstractMetricsQueryModule extends PrivateModule {

  @Override
  protected final void configure() {
    bindMetricsTable();

    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class);
    handlerBinder.addBinding().to(BatchMetricsHandler.class).in(Scopes.SINGLETON);
    handlerBinder.addBinding().to(DeleteMetricsHandler.class).in(Scopes.SINGLETON);

    bind(MetricsQueryService.class).in(Scopes.SINGLETON);
    expose(MetricsQueryService.class);
  }

  @Provides
  @Named(MetricsConstants.ConfigKeys.SERVER_ADDRESS)
  public final InetAddress providesHostname(CConfiguration cConf) {
    return Networks.resolve(cConf.get(MetricsConstants.ConfigKeys.SERVER_ADDRESS),
                            new InetSocketAddress("localhost", 0).getAddress());
  }

  protected abstract void bindMetricsTable();
}
