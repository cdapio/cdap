/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.metrics.collect.LocalMetricsCollectionService;
import com.continuuity.metrics.collect.MetricsCollectionService;
import com.continuuity.metrics.data.HBaseFilterableOVCTableHandle;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.metrics.guice.MetricsQueryRuntimeModule;
import com.continuuity.test.hbase.HBaseTestBase;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class BatchMetricsHandlerTest extends HBaseTestBase {

  private static Injector injector;

  @Test
  @Ignore
  public void testBatchHandler() throws InterruptedException {
    Injector injector = Guice.createInjector(new ConfigModule(),
                                             new DiscoveryRuntimeModule().getInMemoryModules(),
                                             new MetricsClientRuntimeModule().getInMemoryModules(),
                                             new MetricsQueryRuntimeModule().getInMemoryModules());

    MetricsCollectionService collectionService = injector.getInstance(MetricsCollectionService.class);
    MetricsQueryService queryService = injector.getInstance(MetricsQueryService.class);
    queryService.startAndWait();

    try {



      TimeUnit.SECONDS.sleep(1);

    } finally {
      queryService.stopAndWait();
    }
  }

  @BeforeClass
  public static void init() throws Exception {
    HBaseTestBase.startHBase();

    injector = Guice.createInjector(new ConfigModule(CConfiguration.create(), HBaseTestBase.getConfiguration()),
                                    new DiscoveryRuntimeModule().getInMemoryModules(),
                                    new MetricsQueryRuntimeModule().getInMemoryModules(),
                                    // Have the metrics client writes to hbase directly to skips the kafka
                                    new PrivateModule() {
                                      @Override
                                      protected void configure() {
                                        bind(OVCTableHandle.class)
                                          .to(HBaseFilterableOVCTableHandle.class).in(Scopes.SINGLETON);
                                        bind(MetricsCollectionService.class)
                                          .to(LocalMetricsCollectionService.class).in(Scopes.SINGLETON);
                                        expose(MetricsCollectionService.class);
                                      }
                                    });
  }

  @AfterClass
  public static void finish() throws Exception {
    HBaseTestBase.stopHBase();
  }
}
