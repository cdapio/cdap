/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.gateway.handlers.hooks;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.GatewayFastTestsSuite;
import co.cask.cdap.gateway.GatewayTestBase;
import co.cask.cdap.gateway.MockMetricsCollectionService;
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
public class MetricsReporterHookTestRun extends GatewayTestBase {
  private static MockMetricsCollectionService mockMetricsCollectionService;

  @BeforeClass
  public static void init() throws Exception {
    Injector injector = GatewayTestBase.getInjector();
    mockMetricsCollectionService = injector.getInstance(MockMetricsCollectionService.class);
  }

  @Test
  public void testMetricsSuccess() throws Exception {
    String context = Constants.SYSTEM_NAMESPACE + "." + Constants.Service.APP_FABRIC_HTTP + ".PingHandler.ping";
    long received = mockMetricsCollectionService.getMetrics(context, "request.received");
    long successful = mockMetricsCollectionService.getMetrics(context, "response.successful");
    long clientError = mockMetricsCollectionService.getMetrics(context, "response.client-error");

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
    String context = Constants.SYSTEM_NAMESPACE + "." + Constants.Stream.STREAM_HANDLER + ".StreamHandler.getInfo";
    long received = mockMetricsCollectionService.getMetrics(context, "request.received");
    long successful = mockMetricsCollectionService.getMetrics(context, "response.successful");
    long clientError = mockMetricsCollectionService.getMetrics(context, "response.client-error");

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
  private void verifyMetrics(long expected, String context, String metricsName) throws InterruptedException {
    int trial = 0;
    long metrics = -1;
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
