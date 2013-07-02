/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.runtime;

import com.continuuity.app.guice.AppFabricServiceRuntimeModule;
import com.continuuity.app.guice.LocationRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.RemoteOperationExecutor;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.common.Services;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.util.concurrent.Futures;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.name.Names;

import java.util.concurrent.TimeUnit;

/**
 * AppFabric server main. It can be started as regular Java Application
 * or started using apache commons daemon (jsvc)
 */
public final class AppFabricMain extends DaemonMain {

  private ZKClientService zkClientService;
  private AppFabricServer appFabricServer;
  private Injector injector;

  public static void main(final String[] args) throws Exception {
    new AppFabricMain().doMain(args);
  }

  @Override
  public void init(String[] args) {
    CConfiguration cConf = CConfiguration.create();
    zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(cConf.get("zookeeper.quorum")).setSessionTimeout(10000).build(),
            RetryStrategies.fixDelay(2, TimeUnit.SECONDS)
          )
        )
      );

    injector = Guice.createInjector(
      new ConfigModule(),
      new IOModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule(zkClientService).getDistributedModules(),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // Bind the remote opex
          bind(OperationExecutor.class).to(RemoteOperationExecutor.class).in(Singleton.class);
          bind(CConfiguration.class)
            .annotatedWith(Names.named("RemoteOperationExecutorConfig"))
            .to(CConfiguration.class);
        }
      }
    );
  }

  @Override
  public void start() {
    injector.getInstance(WeaveRunnerService.class).startAndWait();
    appFabricServer = injector.getInstance(AppFabricServer.class);
    Futures.getUnchecked(Services.chainStart(zkClientService, appFabricServer));
  }

  /**
   * Invoked by jsvc to stop the program.
   */
  @Override
  public void stop() {
    Futures.getUnchecked(Services.chainStop(appFabricServer, zkClientService));
  }

  /**
   * Invoked by jsvc for resource cleanup
   */
  @Override
  public void destroy() {
  }
}
