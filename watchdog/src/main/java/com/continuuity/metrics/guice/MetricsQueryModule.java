/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.Networks;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.query.MetricsQueryService;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Base guice module for binding metrics query service classes.
 */
public class MetricsQueryModule extends PrivateModule {

  @Override
  protected final void configure() {
    install(new MetricsHandlerModule());
    bind(MetricsQueryService.class).in(Scopes.SINGLETON);
    expose(MetricsQueryService.class);
  }

  @Provides
  @Named(MetricsConstants.ConfigKeys.SERVER_ADDRESS)
  public final InetAddress providesHostname(CConfiguration cConf) {
    return Networks.resolve(cConf.get(MetricsConstants.ConfigKeys.SERVER_ADDRESS),
                            new InetSocketAddress("localhost", 0).getAddress());
  }
}
