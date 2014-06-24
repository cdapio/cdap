package com.continuuity.gateway.handlers.hooks;

import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.gateway.GatewayTestBase;
import com.continuuity.gateway.MockMetricsCollectionService;
import com.google.inject.Injector;
import org.apache.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Test MetricReporterHook.
 */
public class MetricsReporterHookTest extends GatewayTestBase {
  private static MockMetricsCollectionService mockMetricsCollectionService;

  @BeforeClass
  public static void init() throws Exception {
    Injector injector = GatewayTestBase.getInjector();
    mockMetricsCollectionService = injector.getInstance(MockMetricsCollectionService.class);
  }

  @Test
  public void testMetricsSuccess() throws Exception {
    String context = Constants.Service.APP_FABRIC_HTTP + ".PingHandler.ping";
    int received = mockMetricsCollectionService.getMetrics(context, "request.received");
    int successful = mockMetricsCollectionService.getMetrics(context, "response.successful");
    int clientError = mockMetricsCollectionService.getMetrics(context, "response.client-error");

    // Make a successful call
    HttpResponse response = GatewayFastTestsSuite.doGet("/ping");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    // received and successful should have increased by one, clientError should be the same
    verifyMetrics(received + 1, context, "request.received");
    verifyMetrics(successful + 1, context, "response.successful");
    verifyMetrics(clientError, context, "response.client-error");
  }

  @Test
  public void testMetricsNotFound() throws Exception {
    String context = Constants.Stream.STREAM_HANDLER + ".StreamHandler.getInfo";
    int received = mockMetricsCollectionService.getMetrics(context, "request.received");
    int successful = mockMetricsCollectionService.getMetrics(context, "response.successful");
    int clientError = mockMetricsCollectionService.getMetrics(context, "response.client-error");

    // Get info of non-existent stream
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/streams/metrics-hook-test-non-existent-stream/info");
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatusLine().getStatusCode());

    // received and clientError should have increased by one, successful should be the same
    verifyMetrics(received + 1, context, "request.received");
    verifyMetrics(successful, context, "response.successful");
    verifyMetrics(clientError + 1, context, "response.client-error");
  }

  /**
   * Verify metrics. It tries couple times to avoid race condition.
   * This is because metrics hook is updated asynchronously.
   */
  private void verifyMetrics(int expected, String context, String metricsName) throws InterruptedException {
    int trial = 0;
    int metrics = -1;
    while (trial++ < 5) {
      metrics = mockMetricsCollectionService.getMetrics(context, metricsName);
      if (expected == metrics) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertEquals(expected, metrics);
  }
}
