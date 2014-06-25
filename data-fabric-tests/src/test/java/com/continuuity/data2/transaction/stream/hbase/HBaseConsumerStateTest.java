/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream.hbase;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.hbase.HBaseTestFactory;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStore;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStoreFactory;
import com.continuuity.data2.transaction.stream.StreamConsumerStateTestBase;
import com.continuuity.test.SlowTests;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 *
 */
@Category(SlowTests.class)
public class HBaseConsumerStateTest extends StreamConsumerStateTestBase {

  private static HBaseTestBase testHBase;
  private static StreamAdmin streamAdmin;
  private static StreamConsumerStateStoreFactory stateStoreFactory;

  @BeforeClass
  public static void init() throws Exception {
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
    Configuration hConf = testHBase.getConfiguration();
    CConfiguration cConf = CConfiguration.create();

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new TransactionMetricsModule(),
      new DataFabricDistributedModule()
    );
    streamAdmin = injector.getInstance(StreamAdmin.class);
    stateStoreFactory = injector.getInstance(StreamConsumerStateStoreFactory.class);
  }

  @AfterClass
  public static void finish() throws Exception {
    testHBase.stopHBase();
  }

  @Override
  protected StreamConsumerStateStore createStateStore(StreamConfig streamConfig) throws Exception {
    return stateStoreFactory.create(streamConfig);
  }

  @Override
  protected StreamAdmin getStreamAdmin() {
    return streamAdmin;
  }
}
