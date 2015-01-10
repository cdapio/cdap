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

import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MetricsDiscoveryQueryTestRun extends MetricsSuiteTestBase {

  @BeforeClass
  public static void setup() throws InterruptedException {
    setupMetrics();
  }

  @Test
  public void testDiscoverMetrics() throws Exception {
    JsonArray expected = new JsonArray();

    JsonArray readContexts =
      children(
        node("app", "WCount", children(
          node("flow", "WCounter", children(
            node("flowlet", "counter"),
            node("flowlet", "splitter"))),
          node("flow", "WordCounter", children(
            node("flowlet", "splitter"))),
          node("mapreduce", "ClassicWordCount", children(
            node("mapreduceTask", "mappers"),
            node("mapreduceTask", "reducers"))),
          node("procedure", "RCounts"))));

    JsonObject reads = new JsonObject();
    reads.addProperty("metric", "reads");
    reads.add("contexts", readContexts);
    expected.add(reads);

    HttpResponse response = doGet("/v2/metrics/available/apps/WCount");
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    try {
      Assert.assertEquals("did not return 200 status.",
                          HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
      JsonArray json = new Gson().fromJson(reader, JsonArray.class);
      Assert.assertEquals(expected, json);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testFilters() throws Exception {
    JsonArray expected = new JsonArray();
    JsonArray contexts =
      children(
        node("app", "WordCount", children(
          node("flow", "WordCounter", children(
            node("flowlet", "splitter"))))));
    JsonObject expectedReads = new JsonObject();
    expectedReads.addProperty("metric", "reads");
    expectedReads.add("contexts", contexts);
    expected.add(expectedReads);
    expected.add(expectedWrites());

    HttpResponse response = doGet("/v2/metrics/available/apps/WordCount/flows/WordCounter/flowlets/splitter");
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    try {
      Assert.assertEquals("did not return 200 status.",
                          HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
      JsonArray json = new Gson().fromJson(reader, JsonArray.class);
      Assert.assertEquals(expected, json);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testMalformedPathReturns404() throws Exception {
    String base = "/v2/metrics/available";
    String[] resources = {
      base + "/apps/WordCount/flow/WordCounter",
      base + "/apps/WordCount/flows/WordCounter/flowlets",
      base + "/apps/WordCount/flows/WordCounter/flowlet/splitter",
    };
    for (String resource : resources) {
      HttpResponse response = doGet(resource);
      Assert.assertEquals(resource + " did not return 404 as expected.",
                          HttpStatus.SC_NOT_FOUND, response.getStatusLine().getStatusCode());
    }
  }

  private static void setupMetrics() throws InterruptedException {
    MetricsCollector collector =
      collectionService.getCollector(MetricsScope.USER,
                                     getFlowletContext("WordCount", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);
    collector =
      collectionService.getCollector(MetricsScope.USER,
                                     getFlowletContext("WCount", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector =
      collectionService.getCollector(MetricsScope.USER,
                                     getFlowletContext("WCount", "WCounter", "splitter"));
    collector.increment("reads", 1);
    collector =
      collectionService.getCollector(MetricsScope.USER,
                                     getFlowletContext("WCount", "WCounter", "counter"));
    collector.increment("reads", 1);
    collector =
      collectionService.getCollector(MetricsScope.USER,
                                     getProcedureContext("WCount", "RCounts"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER,
                                               getMapReduceTaskContext("WCount", "ClassicWordCount",
                                                                       MapReduceMetrics.TaskType.Mapper));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER,
                                               getMapReduceTaskContext("WCount", "ClassicWordCount",
                                                                       MapReduceMetrics.TaskType.Reducer));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER,
                                               getFlowletContext("WordCount", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);

    // need a better way to do this
    TimeUnit.SECONDS.sleep(2);
  }

  private JsonObject expectedWrites() {
    JsonArray writeContexts =
      children(
        node("app", "WordCount", children(
          node("flow", "WordCounter", children(
            node("flowlet", "splitter"))))));

    JsonObject writes = new JsonObject();
    writes.addProperty("metric", "writes");
    writes.add("contexts", writeContexts);
    return writes;
  }

  private JsonObject node(String type, String id) {
    JsonObject out = new JsonObject();
    out.addProperty("type", type);
    out.addProperty("id", id);
    return out;
  }

  private JsonObject node(String type, String id, JsonArray children) {
    JsonObject out = new JsonObject();
    out.addProperty("type", type);
    out.addProperty("id", id);
    out.add("children", children);
    return out;
  }

  private JsonArray children(JsonObject... objects) {
    JsonArray out = new JsonArray();
    for (JsonObject obj : objects) {
      out.add(obj);
    }
    return out;
  }
}
