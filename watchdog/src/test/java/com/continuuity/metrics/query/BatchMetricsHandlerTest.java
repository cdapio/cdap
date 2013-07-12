/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.metrics.guice.MetricsRuntimeModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class BatchMetricsHandlerTest {

  @Test
  @Ignore
  public void testBatchHandler() throws InterruptedException {
    Injector injector = Guice.createInjector(new ConfigModule(),
                                             new DiscoveryRuntimeModule().getInMemoryModules(),
                                             new MetricsRuntimeModule().getInMemoryModules());

    MetricsQueryService service = injector.getInstance(MetricsQueryService.class);
    service.startAndWait();

    try {

      TimeUnit.SECONDS.sleep(1);

    } finally {
      service.stopAndWait();
    }
  }
}
