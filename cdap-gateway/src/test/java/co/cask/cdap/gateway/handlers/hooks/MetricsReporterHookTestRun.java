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
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Test MetricReporterHook.
 */
public class MetricsReporterHookTestRun extends GatewayTestBase {
  private static final String API_KEY = "SampleTestApiKey";
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.API_KEY, API_KEY);

  @Test
  public void testMetricsSuccess() throws Exception {
    String path = "/system/services/appfabric/handlers/PingHandler/methods/ping";

    long received = getMetricValue(path, "request.received");
    long successful = getMetricValue(path, "response.successful");
    long clientError = getMetricValue(path, "response.client-error");

    // Make a successful call
    HttpResponse response = GatewayFastTestsSuite.doGet("/ping");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    // received and successful should have increased by one, clientError should be the same
    verifyMetrics(received + 1, path, "request.received");
    verifyMetrics(successful + 1, path, "response.successful");
    verifyMetrics(clientError, path, "response.client-error");
  }

  @Test
  public void testMetricsNotFound() throws Exception {
    String path = "/system/services/stream.handler/handlers/StreamHandlerV2/methods/getInfo";

    long received = getMetricValue(path, "request.received");
    long successful = getMetricValue(path, "response.successful");
    long clientError = getMetricValue(path, "response.client-error");

    // Get info of non-existent stream
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/streams/metrics-hook-test-non-existent-stream/info");
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getStatusLine().getStatusCode());

    // received and clientError should have increased by one, successful should be the same
    verifyMetrics(received + 1, path, "request.received");
    verifyMetrics(successful, path, "response.successful");
    verifyMetrics(clientError + 1, path, "response.client-error");
  }

  /**
   * Verify metrics. It tries couple times to avoid race condition.
   * This is because metrics hook is updated asynchronously.
   */
  private void verifyMetrics(long expected, String context, String metricsName) throws Exception {
    int trial = 0;
    long metrics = -1;
    while (trial++ < 5) {
      metrics = getMetricValue(context, metricsName);
      if (expected == metrics) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertEquals(expected, metrics);
  }

  // todo: use v3 apis
  private static long getMetricValue(String contextPath, String metricName) throws Exception {
    HttpResponse response = doGet("/v2/metrics" + contextPath + "/" + metricName + "?aggregate=true");
    Assert.assertEquals("GET " + contextPath + " did not return 200 status.",
                        HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    JsonObject json = new Gson().fromJson(content, JsonObject.class);
    return json.get("data").getAsInt();
  }

  public static HttpResponse doGet(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet(getEndPoint(resource));
    get.setHeader(AUTH_HEADER);
    return client.execute(get);
  }
}
