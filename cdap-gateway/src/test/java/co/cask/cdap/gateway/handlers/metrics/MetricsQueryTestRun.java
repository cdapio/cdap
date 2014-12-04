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
package co.cask.cdap.gateway.handlers.metrics;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.common.queue.QueueName;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MetricsQueryTestRun extends MetricsSuiteTestBase {

  @Test
  public void testQueueLength() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("WordCount", "WordCounter", "counter", "queue");

    // Insert queue metrics
    MetricsCollector enqueueCollector = collectionService.getCollector(MetricsScope.SYSTEM,
                                                                       "WordCount.f.WordCounter.counter", "0");
    enqueueCollector.increment("process.events.out", 10, queueName.getSimpleName());

    // Insert ack metrics
    MetricsCollector ackCollector = collectionService.getCollector(MetricsScope.SYSTEM,
                                                                   "WordCount.f.WordCounter.unique", "0");
    ackCollector.increment("process.events.processed", 6, "input." + queueName.toString());
    ackCollector.increment("process.events.processed", 2, "input.stream:///streamX");
    ackCollector.increment("process.events.processed", 1, "input.stream://developer/streamX");

    // Insert stream metrics
    MetricsCollector streamCollector = collectionService.getCollector(MetricsScope.SYSTEM,
                                                                      Constants.Gateway.METRICS_CONTEXT, "0");
    streamCollector.increment("collect.events", 5, "streamX");

    // Wait for collection to happen
    TimeUnit.SECONDS.sleep(2);

    // Query for queue length
    HttpPost post = getPost("/v2/metrics");
    post.setHeader("Content-type", "application/json");
    post.setEntity(new StringEntity(
      "[\"/system/apps/WordCount/flows/WordCounter/flowlets/unique/process.events.pending?aggregate=true\"]"));
    HttpResponse response = doPost(post);
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    try {
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
      JsonElement json = new Gson().fromJson(reader, JsonElement.class);
      // Expected result looks like
      // [
      //   {
      //     "path":"/process/events/appId/flows/flowId/flowlet2/pending?aggregate=true",
      //     "result":{"data":6}
      //   }
      // ]
      JsonObject resultObj = json.getAsJsonArray().get(0).getAsJsonObject().get("result").getAsJsonObject();
      Assert.assertEquals(6, resultObj.getAsJsonPrimitive("data").getAsInt());
    } finally {
      reader.close();
    }
  }


  @Test
  public void testingSystemMetrics() throws Exception {
    // Insert system metric
    MetricsCollector collector = collectionService.getCollector(MetricsScope.SYSTEM,
                                                                "appfabric.AppFabricHttpHandler.getAllApps",
                                                                "0");
    collector.increment("request.received", 1);

    // Wait for collection to happen
    TimeUnit.SECONDS.sleep(2);

    String methodRequest =
      "/system/services/appfabric/handlers/AppFabricHttpHandler/methods/getAllApps/" +
        "request.received?aggregate=true";
    String handlerRequest =
      "/system/services/appfabric/handlers/AppFabricHttpHandler/request.received?aggregate=true";
    String serviceRequest =
      "/system/services/appfabric/request.received?aggregate=true";

    testSingleMetric(methodRequest, 1);
    testSingleMetric(handlerRequest, 1);
    testSingleMetric(serviceRequest, 1);
  }

  @Test
  public void testingInvalidSystemMetrics() throws Exception {
    //appfabrics service does not exist
    String methodRequest =
      "/system/services/appfabrics/handlers/AppFabricHttpHandler/methods/getAllApps/" +
        "request.received?aggregate=true";

    HttpResponse response = doGet("/v2/metrics" + methodRequest);
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    try {
      Assert.assertEquals("GET " + methodRequest + " did not return 404 NOT-FOUND status.",
                          HttpStatus.SC_NOT_FOUND, response.getStatusLine().getStatusCode());
    } finally {
      reader.close();
    }

  }


  @Test
  public void testingUserServiceMetrics() throws Exception {
    MetricsCollector collector = collectionService.getCollector(MetricsScope.USER,
                                                                "WordCount.u.CounterService.CountRunnable", "0");
    collector.increment("reads", 1);

    // Wait for collection to happen
    TimeUnit.SECONDS.sleep(2);

    String runnableRequest =
      "/user/apps/WordCount/services/CounterService/runnables/CountRunnable/reads?aggregate=true";

    String serviceRequest =
      "/user/apps/WordCount/services/CounterService/reads?aggregate=true";
    testSingleMetric(runnableRequest, 1);
    testSingleMetric(serviceRequest, 1);
  }

  @Test
  public void testingMetricsWithRunIds() throws Exception {
    String runId1 = "id123";
    String runId2 = "id124";
    String runId3 = "id125";

    MetricsCollector collector1 = collectionService.getCollector(MetricsScope.USER,
                                                                "WordCount.u.CounterService.CountRunnable", runId1);
    collector1.increment("rid_metric", 1);

    MetricsCollector collector2 = collectionService.getCollector(MetricsScope.USER,
                                                                "WordCount.u.CounterService.CountRunnable", runId2);
    collector2.increment("rid_metric", 2);

    MetricsCollector collector3 = collectionService.getCollector(MetricsScope.USER,
                                                                 "WordCount.b.CounterMapRed.m", runId3);
    collector3.gauge("entries.out", 10);

    MetricsCollector collector4 = collectionService.getCollector(MetricsScope.USER,
                                                                 "WordCount.b.CounterMapRed.r", runId3);
    collector4.gauge("entries.out", 10);

    // Wait for collection to happen
    TimeUnit.SECONDS.sleep(2);

    String serviceRequest =
      "/user/apps/WordCount/services/CounterService/runs/" + runId2 + "/rid_metric?aggregate=true";

    //service metric request with invliad runId
    String serviceRequestInvalidId =
      "/user/apps/WordCount/services/CounterService/runs/fff/rid_metric?aggregate=true";

    //service metric request without specifying the runId and aggregate will run the sum of these two runIds
    String serviceRequestTotal =
      "/user/apps/WordCount/services/CounterService/rid_metric?aggregate=true";

    String mappersMetric =
      "/user/apps/WordCount/mapreduce/CounterMapRed/runs/" + runId3 + "/mappers/entries.out?aggregate=true";

    String reducersMetric =
      "/user/apps/WordCount/mapreduce/CounterMapRed/runs/" + runId3 + "/reducers/entries.out?aggregate=true";

    String mapredMetric =
      "/user/apps/WordCount/mapreduce/CounterMapRed/runs/" + runId3 + "/entries.out?aggregate=true";


    testSingleMetric(serviceRequest, 2);
    testSingleMetric(serviceRequestInvalidId, 0);
    testSingleMetric(serviceRequestTotal, 3);
    testSingleMetric(mappersMetric, 10);
    testSingleMetric(reducersMetric, 10);
    testSingleMetric(mapredMetric, 20);
  }

  @Test
  public void testMetricsQueryInvalidRunId() throws Exception {
    String runId1 = "id123";
    String runId2 = "id124";

    MetricsCollector collector2 = collectionService.getCollector(MetricsScope.USER,
                                                                 "WordCount.u.CounterService.CountRunnableInvalid",
                                                                 runId2);
    collector2.increment("rid_metric_invalid", 2);

    //runnable metric request with runId1
    String runnableRequest =
      "/user/apps/WordCount/services/CounterService/runnables/CountRunnableInvalid/run-id/" + runId1 + "/run-id/" +
        runId2 + "rid_metric_invalid?aggregate=true";
    HttpResponse response = doGet("/v2/metrics" + runnableRequest);
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testingUserServiceGaugeMetrics() throws Exception {
    MetricsCollector collector = collectionService.getCollector(MetricsScope.USER,
                                                                "WordCount.u.CounterService.CountRunnable", "0");
    collector.increment("gmetric", 1);
    collector.gauge("gmetric", 10);
    collector.increment("gmetric", 1);
    collector.gauge("gmetric", 10);

    // Wait for collection to happen
    TimeUnit.SECONDS.sleep(2);

    String runnableRequest =
      "/user/apps/WordCount/services/CounterService/runnables/CountRunnable/gmetric?aggregate=true";

    String serviceRequest =
      "/user/apps/WordCount/services/CounterService/gmetric?aggregate=true";
    testSingleMetric(runnableRequest, 10);
    testSingleMetric(serviceRequest, 10);
  }

  @Test
  public void testingUserServiceGaugeMetricsTags() throws Exception {
    MetricsCollector collector = collectionService.getCollector(MetricsScope.USER,
                                                                "WordCount.u.CounterService", "0");
    collector.gauge("gtmetric", 10, "tag1");
    collector.gauge("gtmetric", 20, "tag2");

    // Wait for collection to happen
    TimeUnit.SECONDS.sleep(2);

    String serviceRequest =
      "/user/apps/WordCount/services/CounterService/gtmetric?aggregate=true";
    testSingleMetric(serviceRequest, 20);
  }


  @Test
  public void testingInvalidUserServiceMetrics() throws Exception {
    MetricsCollector collector = collectionService.getCollector(MetricsScope.USER,
                                                                "WordCount.u.InvalidService.CountRunnable", "0");
    collector.increment("reads", 1);

    // Wait for collection to happen
    TimeUnit.SECONDS.sleep(2);

    String runnableRequest =
      "/user/apps/WordCount/service/InvalidService/runnables/CountRunnable/reads?aggregate=true";

    HttpResponse response = doGet("/v2/metrics" + runnableRequest);
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    try {
      Assert.assertEquals("GET " + runnableRequest + " did not return 404 status.",
                          HttpStatus.SC_NOT_FOUND, response.getStatusLine().getStatusCode());
    } finally {
      reader.close();
    }
  }

  private static void testSingleMetric(String resource, int value) throws Exception {
    testSingleMetricWithGet(resource, value);
    testSingleMetricWithPost(resource, value);
  }

  private static void testSingleMetricWithGet(String resource, int value) throws Exception {
    HttpResponse response = doGet("/v2/metrics" + resource);
    Assert.assertEquals("GET " + resource + " did not return 200 status.",
                        HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    JsonObject json = new Gson().fromJson(content, JsonObject.class);
    Assert.assertEquals("GET " + resource + " returned unexpected results.", value, json.get("data").getAsInt());
  }

  private static void testSingleMetricWithPost(String resource, int value) throws Exception {
    HttpPost post = getPost("/v2/metrics");
    post.setHeader("Content-type", "application/json");
    post.setEntity(new StringEntity("[\"" + resource + "\"]"));
    HttpResponse response = doPost(post);
    Assert.assertEquals("POST " + resource + " did not return 200 status.",
                        HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    JsonArray json = new Gson().fromJson(content, JsonArray.class);
    // Expected result looks like
    // [
    //   {
    //     "path":"/smth/smth",
    //     "result":{"data":<value>}
    //   }
    // ]
    Assert.assertEquals("POST " + resource + " returned unexpected results.", value,
                        json.get(0).getAsJsonObject().getAsJsonObject("result").get("data").getAsInt());
  }

  @Test
  public void testingTransactoinMetrics() throws Exception {
    // Insert system metric  (stream.handler is the service name)
    MetricsCollector collector = collectionService.getCollector(MetricsScope.SYSTEM, "transactions", "0");
    collector.increment("inprogress", 1);

    // Wait for collection to happen
    TimeUnit.SECONDS.sleep(2);

    String request = "/system/transactions/inprogress?aggregate=true";
    testSingleMetric(request, 1);
  }

  @Test
  public void testGetMetric() throws Exception {
    // Insert some metric
    MetricsCollector collector = collectionService.getCollector(MetricsScope.SYSTEM,
                                                                "WordCount.f.WordCounter.counter", "0");
    collector.increment("reads", 10, "wordStats");
    collector.increment("collect.events", 10, "wordStream");
    collector = collectionService.getCollector(MetricsScope.SYSTEM, "-.cluster", "0");
    collector.increment("resources.total.storage", 10);

    // Wait for collection to happen
    TimeUnit.SECONDS.sleep(2);

    for (String resource : validResources) {
      HttpResponse response = doGet("/v2/metrics" + resource);
      Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
      try {
        Assert.assertEquals("GET " + resource + " did not return 200 status.",
                            HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        JsonObject json = new Gson().fromJson(reader, JsonObject.class);
        // Expected result looks like
        // {
        //   "result":{"data":10}
        // }
        Assert.assertEquals("GET " + resource + " returned unexpected results.", 10, json.get("data").getAsInt());
      } finally {
        reader.close();
      }
    }
  }

  @Test
  public void testNonExistingResourcesReturnZeroes() throws Exception {
    for (String resource : nonExistingResources) {
      testSingleMetric(resource, 0);
    }
  }

  @Test
  public void testMalformedPathReturns404() throws Exception {
    for (String resource : malformedResources) {
      // test GET request fails with 404
      HttpResponse response = doGet("/v2/metrics" + resource);
      Assert.assertEquals("GET " + resource + " did not return 404 as expected.",
                          HttpStatus.SC_NOT_FOUND, response.getStatusLine().getStatusCode());
      // test POST also fails, but with 400
      HttpPost post = getPost("/v2/metrics");
      post.setHeader("Content-type", "application/json");
      post.setEntity(new StringEntity("[\"" + resource + "\"]"));
      response = doPost(post);
      Assert.assertEquals("POST for " + resource + " did not return 400 as expected.",
                          HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode());
    }
  }
}
