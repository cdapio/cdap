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

import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
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

  @Test
  public void testMetricsContexts() throws Exception {
    metricsResponseCheck("/v2/metrics/available/context", 2);
    metricsResponseCheck("/v2/metrics/available/context/WordCount.f", 1);
    metricsResponseCheck("/v2/metrics/available/context/WCount", 3);

    String base = "/v2/metrics/available/context/WCount.f";
    HttpResponse response = doGet(base);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    String result = EntityUtils.toString(response.getEntity());
    List<String> resultList = new Gson().fromJson(result, new TypeToken<List<String>>() { }.getType());
    Assert.assertEquals(2, resultList.size());
    Assert.assertEquals("WCounter", resultList.get(0));
    Assert.assertEquals("WordCounter", resultList.get(1));

  }

  private void metricsResponseCheck(String url, int expected) throws Exception {
    HttpResponse response = doGet(url);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String result = EntityUtils.toString(response.getEntity());
    List<String> reply = new Gson().fromJson(result, new TypeToken<List<String>>() { }.getType());
    Assert.assertEquals(expected, reply.size());
  }

  @Test
  public void testMetrics() throws Exception {
    String base = "/v2/metrics/available/context/WordCount.f.WordCounter.splitter/metrics";
    HttpResponse response = doGet(base);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    String result = EntityUtils.toString(response.getEntity());
    List<String> resultList = new Gson().fromJson(result, new TypeToken<List<String>>() { }.getType());
    Assert.assertEquals(2, resultList.size());

    base = "/v2/metrics/available/context/WordCount.f.WordCounter.collector/metrics";
    response = doGet(base);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    result = EntityUtils.toString(response.getEntity());
    resultList = new Gson().fromJson(result, new TypeToken<List<String>>() { }.getType());
    Assert.assertEquals(3, resultList.size());
    Assert.assertEquals("aa", resultList.get(0));
    Assert.assertEquals("ab", resultList.get(1));
    Assert.assertEquals("zz", resultList.get(2));

  }

  private static void setupMetrics() throws InterruptedException {
    MetricsCollector collector =
      collectionService.getCollector(MetricsScope.USER, "WordCount.f.WordCounter.splitter", "0");
    collector.increment("reads", 1);
    collector.increment("writes", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "WCount.f.WordCounter.splitter", "0");
    collector.increment("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "WCount.f.WCounter.splitter", "0");
    collector.increment("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "WCount.f.WCounter.counter", "0");
    collector.increment("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "WCount.p.RCounts", "0");
    collector.increment("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "WCount.b.ClassicWordCount.m", "0");
    collector.increment("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "WCount.b.ClassicWordCount.r", "0");
    collector.increment("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "WordCount.f.WordCounter.splitter", "0");
    collector.increment("reads", 1);
    collector.increment("writes", 1);

    collector = collectionService.getCollector(MetricsScope.USER, "WordCount.f.WordCounter.collector", "0");
    collector.increment("aa", 1);
    collector.increment("zz", 1);
    collector.increment("ab", 1);

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
