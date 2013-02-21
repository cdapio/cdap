/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.app.services.ServerFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class AppFabricServerTest {
  private static ServerFactory server;
  private static CConfiguration configuration;

  @BeforeClass
  public static void before() throws Exception {
    Injector injector = Guice.createInjector(
      new BigMamaModule(),
      new DataFabricModules().getInMemoryModules()
    );

    configuration = CConfiguration.create();
    configuration.setInt(Constants.CFG_APP_FABRIC_SERVER_PORT, 45000);
    configuration.set("app.output.dir", "/tmp/app");
    configuration.set("app.tmp.dir", "/tmp/temp");
    server = injector.getInstance(ServerFactory.class);
  }

  @Test
  public void startStopServer() throws Exception {
    Service service = server.create(configuration);
    ListenableFuture<Service.State> future = service.start();
    future.cancel(true);
    Thread.sleep(10000);
  }
}
