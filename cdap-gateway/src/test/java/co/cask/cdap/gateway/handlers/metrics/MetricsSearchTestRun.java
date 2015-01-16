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
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Search available contexts and metrics tests
 */
public class MetricsSearchTestRun extends MetricsSuiteTestBase {

  @BeforeClass
  public static void setup() throws Exception {
    setupMetrics();
  }

  private static void setupMetrics() throws Exception {
    HttpResponse response = doDelete("/v2/metrics");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    MetricsCollector collector =
      collectionService.getCollector(getFlowletContext("WordCount", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);
    collector = collectionService.getCollector(getFlowletContext("WCount", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getFlowletContext("WCount", "WCounter", "splitter"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getFlowletContext("WCount", "WCounter", "counter"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getProcedureContext("WCount", "RCounts"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getMapReduceTaskContext("WCount", "ClassicWordCount",
                                                                       MapReduceMetrics.TaskType.Mapper));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(
      getMapReduceTaskContext("WCount", "ClassicWordCount", MapReduceMetrics.TaskType.Reducer));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getFlowletContext("WordCount", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);

    collector = collectionService.getCollector(getFlowletContext("WordCount", "WordCounter", "collector"));
    collector.increment("aa", 1);
    collector.increment("zz", 1);
    collector.increment("ab", 1);

    // need a better way to do this
    TimeUnit.SECONDS.sleep(2);
  }

  @Test
  public void testMetricsContexts() throws Exception {
    metricsResponseCheck("/v3/namespaces/default/metrics/user?search=childContext", 2,
                         ImmutableList.<String>of("WCount", "WordCount"));
    metricsResponseCheck("/v3/namespaces/default/metrics/user/WordCount.f?search=childContext", 1,
                         ImmutableList.<String>of("WordCounter"));
    metricsResponseCheck("/v3/namespaces/default/metrics/user/WCount?search=childContext", 3,
                         ImmutableList.<String>of("b", "f", "p"));
    metricsResponseCheck("/v3/namespaces/default/metrics/user/WCount.b.ClassicWordCount?search=childContext", 2,
                         ImmutableList.<String>of("m", "r"));
    metricsResponseCheck("/v3/namespaces/default/metrics/user/WCount.b.ClassicWordCount.m?search=childContext", 0,
                         ImmutableList.<String>of());
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
    String base = "/v3/namespaces/default/metrics/user/WordCount.f.WordCounter.splitter/metrics";
    HttpResponse response = doGet(base);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    String result = EntityUtils.toString(response.getEntity());
    List<String> resultList = new Gson().fromJson(result, new TypeToken<List<String>>() { }.getType());
    Assert.assertEquals(2, resultList.size());

    base = "/v3/namespaces/default/metrics/user/WordCount.f.WordCounter.collector/metrics";
    response = doGet(base);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    result = EntityUtils.toString(response.getEntity());
    resultList = new Gson().fromJson(result, new TypeToken<List<String>>() { }.getType());
    Assert.assertEquals(3, resultList.size());
    Assert.assertEquals("aa", resultList.get(0));
    Assert.assertEquals("ab", resultList.get(1));
    Assert.assertEquals("zz", resultList.get(2));

  }
}
