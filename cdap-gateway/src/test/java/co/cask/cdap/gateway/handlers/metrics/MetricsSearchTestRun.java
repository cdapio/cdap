/*
 * Copyright © 2015 Cask Data, Inc.
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
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Search available contexts and metrics tests
 */
public class MetricsSearchTestRun extends MetricsSuiteTestBase {

  private static final String MY_SPACE = "myspace";
  private static final String YOUR_SPACE = "yourspace";

  private static final Gson GSON = new Gson();

  private static final Type LIST_TYPE = new TypeToken<List<String>>() { }.getType();

  @BeforeClass
  public static void setup() throws Exception {
    setupMetrics();
  }

  private static void setupMetrics() throws Exception {
    HttpResponse response = doDelete("/v2/metrics");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    // Adding metrics for app "WordCount" in namespace "myspace", "WCount" in "yourspace"
    MetricsCollector collector =
      collectionService.getCollector(MetricsScope.USER, getFlowletContext(MY_SPACE, "WordCount", "WordCounter",
                                                                          "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);
    collector = collectionService.getCollector(MetricsScope.USER,
                                               getFlowletContext(YOUR_SPACE, "WCount", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER,
                                               getFlowletContext(YOUR_SPACE, "WCount", "WCounter", "splitter"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER,
                                               getFlowletContext(YOUR_SPACE, "WCount", "WCounter", "counter"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER,
                                               getProcedureContext(YOUR_SPACE, "WCount", "RCounts"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER,
                                               getMapReduceTaskContext(YOUR_SPACE, "WCount", "ClassicWordCount",
                                                                       MapReduceMetrics.TaskType.Mapper));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER,
                                               getMapReduceTaskContext(YOUR_SPACE, "WCount", "ClassicWordCount",
                                                                       MapReduceMetrics.TaskType.Reducer));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER,
                                               getFlowletContext(MY_SPACE, "WordCount", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);

    collector = collectionService.getCollector(MetricsScope.USER,
                                               getFlowletContext(MY_SPACE, "WordCount", "WordCounter", "collector"));
    collector.increment("aa", 1);
    collector.increment("zz", 1);
    collector.increment("ab", 1);

    // need a better way to do this
    TimeUnit.SECONDS.sleep(2);
  }

  @Test
  public void testMetricsContexts() throws Exception {
    // WordCount is in myspace, WCount in yourspace
    metricsResponseCheck(getMetricsSearchUri(MY_SPACE, null), 1, ImmutableList.of("WordCount"));
    metricsResponseCheck(getMetricsSearchUri(YOUR_SPACE, null), 1, ImmutableList.of("WCount"));

    // WordCount should be found in myspace, not in yourspace
    metricsResponseCheck(getMetricsSearchUri(MY_SPACE, "WordCount.f"), 1, ImmutableList.of("WordCounter"));
    metricsResponseCheck(getMetricsSearchUri(YOUR_SPACE, "WordCount.f"), 0, ImmutableList.<String>of());

    // WCount should be found in yourspace, not in myspace
    metricsResponseCheck(getMetricsSearchUri(YOUR_SPACE, "WCount"), 3, ImmutableList.of("b", "f", "p"));
    metricsResponseCheck(getMetricsSearchUri(MY_SPACE, "WCount"), 0, ImmutableList.<String>of());

    // verify other metrics for WCount app
    metricsResponseCheck(getMetricsSearchUri(YOUR_SPACE, "WCount.b.ClassicWordCount"), 2, ImmutableList.of("m", "r"));
    metricsResponseCheck(getMetricsSearchUri(YOUR_SPACE, "WCount.b.ClassicWordCount.m"), 0, ImmutableList.<String>of());
  }

  private void metricsResponseCheck(String url, int expected, List<String> expectedValues) throws Exception {
    HttpResponse response = doGet(url);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String result = EntityUtils.toString(response.getEntity());
    List<String> reply = new Gson().fromJson(result, new TypeToken<List<String>>() { }.getType());
    Assert.assertEquals(expected, reply.size());
    for (int i = 0; i < expectedValues.size(); i++) {
      Assert.assertEquals(expectedValues.get(i), reply.get(i));
    }
  }

  @Test
  public void testMetrics() throws Exception {
    String base = getMetricsUri(MY_SPACE, "WordCount.f.WordCounter.splitter");
    HttpResponse response = doGet(base);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    String result = EntityUtils.toString(response.getEntity());
    List<String> resultList = new Gson().fromJson(result, LIST_TYPE);
    Assert.assertEquals(2, resultList.size());

    base = getMetricsUri(MY_SPACE, "WordCount.f.WordCounter.collector");
    response = doGet(base);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    result = EntityUtils.toString(response.getEntity());
    resultList = GSON.fromJson(result, LIST_TYPE);
    Assert.assertEquals(3, resultList.size());
    Assert.assertEquals("aa", resultList.get(0));
    Assert.assertEquals("ab", resultList.get(1));
    Assert.assertEquals("zz", resultList.get(2));

    // make sure that WordCount is not found in yourspace
    base = getMetricsUri(YOUR_SPACE, "WordCount.f.WordCounter.collector");
    response = doGet(base);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    result = EntityUtils.toString(response.getEntity());
    resultList = GSON.fromJson(result, LIST_TYPE);
    Assert.assertEquals(0, resultList.size());

    // test WCount
    base = getMetricsUri(YOUR_SPACE, "WCount.b.ClassicWordCount.m");
    response = doGet(base);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    result = EntityUtils.toString(response.getEntity());
    resultList = GSON.fromJson(result, LIST_TYPE);
    Assert.assertEquals(1, resultList.size());
    Assert.assertEquals("reads", resultList.get(0));

    // WCount should not be found in myspace
    base = getMetricsUri(MY_SPACE, "WCount.b.ClassicWordCount.m");
    response = doGet(base);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    result = EntityUtils.toString(response.getEntity());
    resultList = GSON.fromJson(result, LIST_TYPE);
    Assert.assertEquals(0, resultList.size());
  }

  private static String getMetricsSearchUri(String namespaceId, @Nullable String prefix) {
    String metricsSearchUri;
    if (prefix == null) {
      metricsSearchUri = String.format("/v3/namespaces/%s/metrics/user?search=childContext", namespaceId);
    } else {
      metricsSearchUri = String.format("/v3/namespaces/%s/metrics/user/%s?search=childContext", namespaceId, prefix);
    }
    return metricsSearchUri;
  }

  private static String getMetricsUri(String namespaceId, String metric) {
    return String.format("/v3/namespaces/%s/metrics/user/%s/metrics", namespaceId, metric);
  }
}
