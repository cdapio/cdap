/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.metrics.guice.MetricsQueryRuntimeModule;
import com.continuuity.metrics.query.MetricsQueryService;
import com.continuuity.weave.common.Services;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.concurrent.TimeUnit;

/**
 * Main class for metrics query server.
 */
public final class MetricsQueryMain extends DaemonMain {

  private ZKClientService zkClientService;
  private MetricsQueryService queryService;

  public static void main(String[] args) throws Exception {
    new MetricsQueryMain().doMain(args);
  }

  @Override
  public void init(String[] args) {
    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = HBaseConfiguration.create();

    // Zookeeper for discovery service
    zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(
              cConf.get(Constants.Zookeeper.QUORUM)
            ).setSessionTimeout(10000).build(),
            RetryStrategies.fixDelay(2, TimeUnit.SECONDS)
          )
        )
      );

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new DiscoveryRuntimeModule(zkClientService).getDistributedModules(),
      new MetricsQueryRuntimeModule().getDistributedModules()
    );

    queryService = injector.getInstance(MetricsQueryService.class);
  }

  @Override
  public void start() {
    Futures.getUnchecked(Services.chainStart(zkClientService, queryService));
  }

  @Override
  public void stop() {
    Futures.getUnchecked(Services.chainStop(queryService, zkClientService));
  }

  @Override
  public void destroy() {
    // No-op
  }
}
