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
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Search available contexts and metrics tests
 */
public class MetricsHandlerTestRun extends MetricsSuiteTestBase {

  private static long emitTs;

  @BeforeClass
  public static void setup() throws Exception {
    setupMetrics();
  }

  private static void setupMetrics() throws Exception {
    MetricsCollector collector =
      collectionService.getCollector(getFlowletContext("WordCount1", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);
    collector = collectionService.getCollector(getFlowletContext("WCount1", "WordCounter", "splitter"));
    collector.increment("reads", 1);

    collector = collectionService.getCollector(getFlowletContext("WCount1", "WCounter", "splitter"));
    emitTs = System.currentTimeMillis();
    // we want to emit in two different seconds
    collector.increment("reads", 1);
    TimeUnit.MILLISECONDS.sleep(2000);
    collector.increment("reads", 2);

    collector = collectionService.getCollector(getFlowletContext("WCount1", "WCounter", "counter"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getProcedureContext("WCount1", "RCounts"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getMapReduceTaskContext("WCount1", "ClassicWordCount",
                                                                       MapReduceMetrics.TaskType.Mapper));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(
      getMapReduceTaskContext("WCount1", "ClassicWordCount", MapReduceMetrics.TaskType.Reducer));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getFlowletContext("WordCount1", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);

    collector = collectionService.getCollector(getFlowletContext("WordCount1", "WordCounter", "collector"));
    collector.increment("aa", 1);
    collector.increment("zz", 1);
    collector.increment("ab", 1);

    // also: user metrics
    Metrics userMetrics = new ProgramUserMetrics(
      collectionService.getCollector(getFlowletContext("WordCount1", "WordCounter", "splitter")));
    userMetrics.count("reads", 1);
    userMetrics.count("writes", 2);

    // need a better way to do this
    TimeUnit.SECONDS.sleep(2);
  }

  @Test
  public void testSearchContext() throws Exception {
    verifySearchResult("/v3/metrics/search?target=childContext&context=WordCount1.f",
                       ImmutableList.<String>of("WordCounter"));
    verifySearchResult("/v3/metrics/search?target=childContext&context=WCount1",
                       ImmutableList.<String>of("b", "f", "p"));
    verifySearchResult("/v3/metrics/search?target=childContext&context=WCount1.b.ClassicWordCount",
                       ImmutableList.<String>of("m", "r"));
    verifySearchResult("/v3/metrics/search?target=childContext&context=WCount1.b.ClassicWordCount.m",
                       ImmutableList.<String>of());
  }

  @Test
  public void testQueryMetrics() throws Exception {
    // aggregate result
    verifyAggregateQueryResult(
      "/v3/metrics/query?context=WCount1.f.WCounter.splitter&metric=system.reads&aggregate=true", 3);
    verifyAggregateQueryResult(
      "/v3/metrics/query?context=WCount1.f.WCounter.counter&metric=system.reads&aggregate=true", 1);

    // time range
    // now-60s, now+60s
    verifyRangeQueryResult(
      "/v3/metrics/query?context=WCount1.f.WCounter.splitter&metric=system.reads&start=now%2D60s&end=now%2B60s", 2, 3);
    // note: times are in seconds, hence "divide by 1000";
    long start = (emitTs - 60 * 1000) / 1000;
    long end = (emitTs + 60 * 1000) / 1000;
    verifyRangeQueryResult(
      "/v3/metrics/query?context=WCount1.f.WCounter.splitter&metric=system.reads&start=" + start + "&end=" + end, 2, 3);
  }

  @Test
  public void testSearchMetrics() throws Exception {
    verifySearchResult("/v3/metrics/search?target=metric&context=WordCount1.f.WordCounter.splitter",
                       ImmutableList.<String>of("system.reads", "system.writes", "user.reads", "user.writes"));

    verifySearchResult("/v3/metrics/search?target=metric&context=WordCount1.f.WordCounter.collector",
                       ImmutableList.<String>of("system.aa", "system.ab", "system.zz"));
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
