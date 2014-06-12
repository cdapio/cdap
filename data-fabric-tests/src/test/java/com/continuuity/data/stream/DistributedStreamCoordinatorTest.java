/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Tests for {@link DistributedStreamCoordinator}
 */
public class DistributedStreamCoordinatorTest extends StreamCoordinatorTestBase {

  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClient;
  private static Injector injector;

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());

    injector = Guice.createInjector(
      new ConfigModule(cConf),
      new ZKClientModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new LocationRuntimeModule().getDistributedModules()
    );

    zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();
  }

  @Override
  protected StreamCoordinator createStreamCoordinator() {
    return injector.getInstance(StreamCoordinator.class);
  }

  @AfterClass
  public static void finish() {
    zkClient.stopAndWait();
    zkServer.stopAndWait();
  }
}
