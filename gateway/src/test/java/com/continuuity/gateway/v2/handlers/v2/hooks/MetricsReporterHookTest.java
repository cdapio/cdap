package com.continuuity.gateway.v2.handlers.v2.hooks;

import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.gateway.MockMetricsCollectionService;
import com.google.inject.Injector;
import org.apache.http.HttpResponse;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test MetricReporterHook.
 */
public class MetricsReporterHookTest {
  private static MockMetricsCollectionService mockMetricsCollectionService;

  @BeforeClass
  public static void init() throws Exception {
    Injector injector = GatewayFastTestsSuite.getInjector();
    mockMetricsCollectionService = injector.getInstance(MockMetricsCollectionService.class);
  }

  @Ignore
  @Test
  public void testMetrics() throws Exception {
    int received = mockMetricsCollectionService.getMetrics("", "");

    HttpResponse response = GatewayFastTestsSuite.doGet("");
  }
}
