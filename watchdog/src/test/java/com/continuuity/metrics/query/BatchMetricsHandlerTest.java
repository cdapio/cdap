/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.api.metrics.MetricsCollectionService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.metrics.data.HBaseFilterableOVCTableHandle;
import com.continuuity.metrics.guice.AbstractMetricsTableModule;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.metrics.guice.MetricsQueryRuntimeModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class BatchMetricsHandlerTest { //extends HBaseTestBase {

  private static Injector injector;

  @Test
  @Ignore
  public void testBatchHandler() throws InterruptedException {
    MetricsCollectionService collectionService = injector.getInstance(MetricsCollectionService.class);
    collectionService.startAndWait();

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
    final Module tableModule = new AbstractMetricsTableModule() {
      @Override
      protected void bindTableHandle() {
        bind(OVCTableHandle.class)
          .to(HBaseFilterableOVCTableHandle.class).in(Scopes.SINGLETON);
      }
    };
    CConfiguration cConf = CConfiguration.create();
    cConf.set("metrics.query.server.port", "56883");
    injector = Guice.createInjector(new ConfigModule(cConf), //, HBaseTestBase.getConfiguration()),
                                    new DiscoveryRuntimeModule().getInMemoryModules(),
                                    new MetricsQueryRuntimeModule().getSingleNodeModules(),
                                    new MetricsClientRuntimeModule().getSingleNodeModules());
//                                    new AbstractMetricsQueryModule() {
//                                      @Override
//                                      protected void bindMetricsTable() {
//                                        install(tableModule);
//                                      }
//                                    },
//                                    // Have the metrics client writes to hbase directly to skips the kafka
//                                    new PrivateModule() {
//                                      @Override
//                                      protected void configure() {
//                                        install(tableModule);
//                                        bind(MetricsCollectionService.class)
//                                          .to(LocalMetricsCollectionService.class).in(Scopes.SINGLETON);
//                                        expose(MetricsCollectionService.class);
//                                      }
//                                    });
//    HBaseTestBase.startHBase();
  }

  @AfterClass
  public static void finish() throws Exception {
//    HBaseTestBase.stopHBase();
  }
}
