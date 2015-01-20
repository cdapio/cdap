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

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.common.metrics.MetricsCollector;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Search available contexts and metrics tests
 */
public class MetricsHandlerTestRun extends MetricsSuiteTestBase {

  private static long emitTs;

  private static final String MY_SPACE = "myspace";
  private static final String YOUR_SPACE = "yourspace";

  @BeforeClass
  public static void setup() throws Exception {
    setupMetrics();
  }

  private static void setupMetrics() throws Exception {
    // Adding metrics for app "WordCount1" in namespace "myspace", "WCount1" in "yourspace"
    MetricsCollector collector =
      collectionService.getCollector(getFlowletContext(MY_SPACE, "WordCount1", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);
    collector = collectionService.getCollector(getFlowletContext(YOUR_SPACE, "WCount1", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getFlowletContext(YOUR_SPACE, "WCount1", "WCounter", "splitter"));
    emitTs = System.currentTimeMillis();
    // we want to emit in two different seconds
    collector.increment("reads", 1);
    TimeUnit.MILLISECONDS.sleep(2000);
    collector.increment("reads", 2);

    collector = collectionService.getCollector(getFlowletContext(YOUR_SPACE, "WCount1", "WCounter", "counter"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getProcedureContext(YOUR_SPACE, "WCount1", "RCounts"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getMapReduceTaskContext(YOUR_SPACE, "WCount1", "ClassicWordCount",
                                                                       MapReduceMetrics.TaskType.Mapper));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(
      getMapReduceTaskContext(YOUR_SPACE, "WCount1", "ClassicWordCount", MapReduceMetrics.TaskType.Reducer));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getFlowletContext(MY_SPACE, "WordCount1", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);

    collector = collectionService.getCollector(getFlowletContext(MY_SPACE, "WordCount1", "WordCounter", "collector"));
    collector.increment("aa", 1);
    collector.increment("zz", 1);
    collector.increment("ab", 1);

    // also: user metrics
    Metrics userMetrics = new ProgramUserMetrics(
      collectionService.getCollector(getFlowletContext(MY_SPACE, "WordCount1", "WordCounter", "splitter")));
    userMetrics.count("reads", 1);
    userMetrics.count("writes", 2);

    // need a better way to do this
    TimeUnit.SECONDS.sleep(2);
  }

  @Test
  public void testSearchContext() throws Exception {
    // WordCount is in myspace, WCount in yourspace
    verifySearchResult(getContextSearchUri(MY_SPACE, null), ImmutableList.<String>of("WordCount1"));
    verifySearchResult(getContextSearchUri(YOUR_SPACE, null), ImmutableList.<String>of("WCount1"));

    // WordCount should be found in myspace, not in yourspace
    verifySearchResult(getContextSearchUri(MY_SPACE, "WordCount1.f"), ImmutableList.<String>of("WordCounter"));
    verifySearchResult(getContextSearchUri(YOUR_SPACE, "WordCount1.f"), ImmutableList.<String>of());

    // WCount should be found in yourspace, not in myspace
    verifySearchResult(getContextSearchUri(YOUR_SPACE, "WCount1"), ImmutableList.<String>of("b", "f", "p"));
    verifySearchResult(getContextSearchUri(MY_SPACE, "WCount1"), ImmutableList.<String>of());

    // verify other metrics for WCount app
    verifySearchResult(getContextSearchUri(YOUR_SPACE, "WCount1.b.ClassicWordCount"),
                       ImmutableList.<String>of("m", "r"));
    verifySearchResult(getContextSearchUri(YOUR_SPACE, "WCount1.b.ClassicWordCount.m"), ImmutableList.<String>of());
  }

  @Test
  public void testQueryMetrics() throws Exception {
    // aggregate result, in the right namespace
    verifyAggregateQueryResult(
      getMetricsAggregateQueryUri(YOUR_SPACE, "WCount1.f.WCounter.splitter", "system.reads"), 3);
    verifyAggregateQueryResult(
      getMetricsAggregateQueryUri(YOUR_SPACE, "WCount1.f.WCounter.counter", "system.reads"), 1);

    // aggregate result, in the wrong namespace
    verifyAggregateQueryResult(getMetricsAggregateQueryUri(MY_SPACE, "WCount1.f.WCounter.splitter", "system.reads"), 0);

    // time range
    // now-60s, now+60s
    verifyRangeQueryResult(getMetricsRangeQueryUri(YOUR_SPACE, "WCount1.f.WCounter.splitter", "system.reads",
                                                   "now%2D60s", "now%2B60s"), 2, 3);
    // note: times are in seconds, hence "divide by 1000";
    String start = String.valueOf((emitTs - 60 * 1000) / 1000);
    String end = String.valueOf((emitTs + 60 * 1000) / 1000);
    verifyRangeQueryResult(getMetricsRangeQueryUri(YOUR_SPACE, "WCount1.f.WCounter.splitter", "system.reads",
                                                   start, end), 2, 3);
    // range query, in the wrong namespace
    verifyRangeQueryResult(getMetricsRangeQueryUri(MY_SPACE, "WCount1.f.WCounter.splitter", "system.reads",
                                                   start, end), 0, 0);
  }

  @Test
  public void testSearchMetrics() throws Exception {
    // metrics in myspace
    verifySearchResult(getMetricsSearchUri(MY_SPACE, "WordCount1.f.WordCounter.splitter"),
                       ImmutableList.<String>of("system.reads", "system.writes", "user.reads", "user.writes"));

    verifySearchResult(getMetricsSearchUri(MY_SPACE, "WordCount1.f.WordCounter.collector"),
                       ImmutableList.<String>of("system.aa", "system.ab", "system.zz"));

    // wrong namespace
    verifySearchResult(getMetricsSearchUri(YOUR_SPACE, "WordCount1.f.WordCounter.splitter"),
                       ImmutableList.<String>of());


    // metrics in yourspace
    verifySearchResult(getMetricsSearchUri(YOUR_SPACE, "WCount1.f.WCounter.splitter"),
                       ImmutableList.<String>of("system.reads"));

    // wrong namespace
    verifySearchResult(getMetricsSearchUri(MY_SPACE, "WCount1.f.WCounter.splitter"), ImmutableList.<String>of());
  }

  private void verifyAggregateQueryResult(String url, long expectedValue) throws Exception {
    Assert.assertEquals(expectedValue, post(url, AggregateQueryResult.class).data);
  }

  private void verifyRangeQueryResult(String url, long nonZeroPointsCount, long expectedSum) throws Exception {
    TimeRangeQueryResult result = post(url, TimeRangeQueryResult.class);
    for (DataPoint point : result.data) {
      if (point.value != 0) {
        nonZeroPointsCount--;
        expectedSum -= point.value;
      }
    }

    Assert.assertEquals(0, nonZeroPointsCount);
    Assert.assertEquals(0, expectedSum);
  }

  private <T> T post(String url, Class<T> clazz) throws Exception {
    HttpResponse response = doPost(url, null);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String result = EntityUtils.toString(response.getEntity());
    return new Gson().fromJson(result, clazz);
  }

  private void verifySearchResult(String url, List<String> expectedValues) throws Exception {
    HttpResponse response = doPost(url, null);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String result = EntityUtils.toString(response.getEntity());
    List<String> reply = new Gson().fromJson(result, new TypeToken<List<String>>() { }.getType());
    Assert.assertEquals(expectedValues.size(), reply.size());
    for (int i = 0; i < expectedValues.size(); i++) {
      Assert.assertEquals(expectedValues.get(i), reply.get(i));
    }
  }

  private static String getContextSearchUri(String namespaceId, @Nullable String contextPrefix) {
    Preconditions.checkNotNull(namespaceId);
    contextPrefix = (contextPrefix == null) ? namespaceId : String.format("%s.%s", namespaceId, contextPrefix);
    return String.format("/v3/metrics/search?target=childContext&context=%s", contextPrefix);
  }

  private static String getMetricsAggregateQueryUri(String namespaceId, String context, String metric) {
    Preconditions.checkNotNull(namespaceId);
    Preconditions.checkNotNull(context);
    context = String.format("%s.%s", namespaceId, context);
    return String.format("/v3/metrics/query?context=%s&metric=%s&aggregate=true", context, metric);
  }

  private static String getMetricsRangeQueryUri(String namespaceId, String context, String metric, String start,
                                                String end) {
    Preconditions.checkNotNull(namespaceId);
    Preconditions.checkNotNull(context);
    Preconditions.checkNotNull(metric);
    Preconditions.checkNotNull(start);
    Preconditions.checkNotNull(end);
    context = String.format("%s.%s", namespaceId, context);
    return String.format("/v3/metrics/query?context=%s&metric=%s&start=%s&end=%s", context, metric, start, end);
  }

  private String getMetricsSearchUri(String namespaceId, String context) {
    Preconditions.checkNotNull(namespaceId);
    context = (context == null) ? namespaceId : String.format("%s.%s", namespaceId, context);
    return String.format("/v3/metrics/search?target=metric&context=%s", context);
  }

  // helpers to easier json results parsing & verification

  private static final class TimeRangeQueryResult {
    private long start;
    private long end;
    private DataPoint[] data;
  }
  
  private static final class DataPoint {
    private long time;
    private long value;
  }

  private static final class AggregateQueryResult {
    private long data;
  }
}
