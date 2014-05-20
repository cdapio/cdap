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
    Assert.assertEquals(received + 1, mockMetricsCollectionService.getMetrics(context, "request.received"));
    Assert.assertEquals(successful + 1, mockMetricsCollectionService.getMetrics(context, "response.successful"));
    Assert.assertEquals(clientError, mockMetricsCollectionService.getMetrics(context, "response.client-error"));
  }

  @Test
  public void testMetricsNotFound() throws Exception {
    String context = Constants.Service.STREAM_HANDLER + ".StreamHandler.getInfo";
    int received = mockMetricsCollectionService.getMetrics(context, "request.received");
    int successful = mockMetricsCollectionService.getMetrics(context, "response.successful");
    int clientError = mockMetricsCollectionService.getMetrics(context, "response.client-error");

    // Get info of non-existent stream
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/streams/metrics-hook-test-non-existent-stream/info");
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatusLine().getStatusCode());

    // received and clientError should have increased by one, successful should be the same
    Assert.assertEquals(received + 1, mockMetricsCollectionService.getMetrics(context, "request.received"));
    Assert.assertEquals(successful, mockMetricsCollectionService.getMetrics(context, "response.successful"));
    Assert.assertEquals(clientError + 1, mockMetricsCollectionService.getMetrics(context, "response.client-error"));
  }
}
