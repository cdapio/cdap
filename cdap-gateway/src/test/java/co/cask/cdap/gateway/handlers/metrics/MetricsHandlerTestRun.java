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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.metrics.query.MetricQueryResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Search available contexts and metrics tests
 */
public class MetricsHandlerTestRun extends MetricsSuiteTestBase {

  private static long emitTs;

  @Before
  public void setup() throws Exception {
    setupMetrics();
  }

  private static void setupMetrics() throws Exception {
    // Adding metrics for app "WordCount1" in namespace "myspace", "WCount1" in "yourspace"
    MetricsCollector collector =
      collectionService.getCollector(getFlowletContext("myspace", "WordCount1", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);
    collector = collectionService.getCollector(getFlowletContext("yourspace", "WCount1", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getFlowletContext("yourspace", "WCount1", "WCounter", "splitter"));
    emitTs = System.currentTimeMillis();
    // we want to emit in two different seconds
    // todo : figure out why we need this
    TimeUnit.SECONDS.sleep(1);
    collector.increment("reads", 1);
    TimeUnit.MILLISECONDS.sleep(2000);
    collector.increment("reads", 2);

    collector = collectionService.getCollector(getFlowletContext("yourspace", "WCount1", "WCounter", "counter"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getProcedureContext("yourspace", "WCount1", "RCounts"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getMapReduceTaskContext("yourspace", "WCount1", "ClassicWordCount",
                                                                       MapReduceMetrics.TaskType.Mapper,
                                                                       "run1", "task1"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(
      getMapReduceTaskContext("yourspace", "WCount1", "ClassicWordCount",
                              MapReduceMetrics.TaskType.Reducer, "run1", "task2"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getFlowletContext("myspace", "WordCount1", "WordCounter", "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);

    collector = collectionService.getCollector(getFlowletContext("myspace", "WordCount1", "WordCounter", "collector"));
    collector.increment("aa", 1);
    collector.increment("zz", 1);
    collector.increment("ab", 1);

    // also: user metrics
    Metrics userMetrics = new ProgramUserMetrics(
      collectionService.getCollector(getFlowletContext("myspace", "WordCount1", "WordCounter", "splitter")));
    userMetrics.count("reads", 1);
    userMetrics.count("writes", 2);

    collector = collectionService.getCollector(ImmutableMap.of(Constants.Metrics.Tag.CLUSTER_METRICS, "true"));
    collector.increment("resources.total.storage", 10);

    // need a better way to do this
    TimeUnit.SECONDS.sleep(2);
  }

  @Test
  public void testSearchContext() throws Exception {
    // empty context
    verifySearchResultContains("/v3/metrics/search?target=childContext",
                               ImmutableList.<String>of("myspace", "yourspace"));

    // WordCount is in myspace, WCount in yourspace
    verifySearchResult("/v3/metrics/search?target=childContext&context=yourspace",
                       ImmutableList.<String>of("yourspace.WCount1"));
    // WordCount is in myspace, WCount in yourspace
    verifySearchResult("/v3/metrics/search?target=childContext&context=myspace",
                       ImmutableList.<String>of("myspace.WordCount1"));
    verifySearchResult("/v3/metrics/search?target=childContext&context=yourspace",
                       ImmutableList.<String>of("yourspace.WCount1"));

    // WordCount should be found in myspace, not in yourspace
    verifySearchResult("/v3/metrics/search?target=childContext&context=myspace.WordCount1.f",
                       ImmutableList.<String>of("myspace.WordCount1.f.WordCounter"));
    verifySearchResult("/v3/metrics/search?target=childContext&context=yourspace.WordCount1.f",
                       ImmutableList.<String>of());

    // WCount should be found in yourspace, not in myspace
    verifySearchResult("/v3/metrics/search?target=childContext&context=yourspace.WCount1",
                       ImmutableList.<String>of("yourspace.WCount1.b", "yourspace.WCount1.f", "yourspace.WCount1.p"));
    verifySearchResult("/v3/metrics/search?target=childContext&context=myspace.WCount1", ImmutableList.<String>of());

    // verify other metrics for WCount app
    verifySearchResult("/v3/metrics/search?target=childContext&context=yourspace.WCount1.b.ClassicWordCount",
                       ImmutableList.<String>of("yourspace.WCount1.b.ClassicWordCount.m",
                                                "yourspace.WCount1.b.ClassicWordCount.r"));
    verifySearchResult("/v3/metrics/search?target=childContext&context=yourspace.WCount1.b.ClassicWordCount.m",
                       ImmutableList.<String>of("yourspace.WCount1.b.ClassicWordCount.m.task1"));
    verifySearchResult("/v3/metrics/search?target=childContext&context=yourspace.WCount1.b.ClassicWordCount.m.task1",
                       ImmutableList.<String>of());
  }


  @Test
  public void testQueryMetrics() throws Exception {

    //aggregate result, in the right namespace
    verifyAggregateQueryResult(
      "/v3/metrics/query?context=" + getContext("yourspace", "WCount1", "f", "WCounter", "splitter") +
        "&metric=system.reads&aggregate=true", 3);
    verifyAggregateQueryResult(
      "/v3/metrics/query?context=" + getContext("yourspace", "WCount1", "f", "WCounter", "counter") +
        "&metric=system.reads&aggregate=true", 1);

    // aggregate result, in the wrong namespace
    verifyEmptyQueryResult(
      "/v3/metrics/query?context=" + getContext("myspace", "WCount1", "f", "WCounter", "splitter") +
        "&metric=system.reads&aggregate=true");

      // time range
      // now-60s, now+60s
      verifyRangeQueryResult(
        "/v3/metrics/query?context=" + getContext("yourspace", "WCount1", "f", "WCounter", "splitter") +
        "&metric=system.reads&start=now%2D60s&end=now%2B60s", 2, 3);

    // note: times are in seconds, hence "divide by 1000";
    long start = (emitTs - 60 * 1000) / 1000;
    long end = (emitTs + 60 * 1000) / 1000;
    verifyRangeQueryResult(
      "/v3/metrics/query?context=" + getContext("yourspace", "WCount1", "f", "WCounter", "splitter") +
        "&metric=system.reads&start=" + start + "&end="
        + end, 2, 3);
    // range query, in the wrong namespace
    verifyEmptyQueryResult(
      "/v3/metrics/query?context=" + getContext("myspace", "WCount1", "f", "WCounter", "splitter") +
        "&metric=system.reads&start=" + start + "&end="
        + end);
  }

  private void verifyEmptyQueryResult(String url) throws Exception {
    MetricQueryResult queryResult = post(url, MetricQueryResult.class);
    Assert.assertEquals(0, queryResult.getSeries().length);
  }

  private String getContext(String nameSpace, String appName, String programType,
                          String programName, String flowletName) {
    return Constants.Metrics.Tag.NAMESPACE + "." + nameSpace + "." + Constants.Metrics.Tag.APP + "." + appName + "." +
      Constants.Metrics.Tag.PROGRAM_TYPE + "." + programType + "." + Constants.Metrics.Tag.PROGRAM + "." +
      programName + "." + Constants.Metrics.Tag.FLOWLET + "." + flowletName;
  }

  @Test
  public void testSearchMetrics() throws Exception {
    // metrics in myspace
    verifySearchResult("/v3/metrics/search?target=metric&context=myspace.WordCount1.f.WordCounter.splitter",
                       ImmutableList.<String>of("system.reads", "system.writes", "user.reads", "user.writes"));

    verifySearchResult("/v3/metrics/search?target=metric&context=myspace.WordCount1.f.WordCounter.collector",
                       ImmutableList.<String>of("system.aa", "system.ab", "system.zz"));

    verifySearchResult("/v3/metrics/search?target=metric&context=myspace.WordCount1.f.WordCounter",
                       ImmutableList.<String>of("system.aa", "system.ab", "system.reads",
                                                "system.writes", "system.zz", "user.reads", "user.writes"));

    // wrong namespace
    verifySearchResult("/v3/metrics/search?target=metric&context=yourspace.WordCount1.f.WordCounter.splitter",
                       ImmutableList.<String>of());


    // metrics in yourspace
    verifySearchResult("/v3/metrics/search?target=metric&context=yourspace.WCount1.f.WCounter.splitter",
                       ImmutableList.<String>of("system.reads"));

    // wrong namespace
    verifySearchResult("/v3/metrics/search?target=metric&context=myspace.WCount1.f.WCounter.splitter",
                       ImmutableList.<String>of());
  }

  private void verifyAggregateQueryResult(String url, long expectedValue) throws Exception {
    MetricQueryResult queryResult = post(url, MetricQueryResult.class);
    Assert.assertEquals(expectedValue, queryResult.getSeries()[0].getData()[0].getValue());
  }

  private void verifyRangeQueryResult(String url, long nonZeroPointsCount, long expectedSum) throws Exception {
    MetricQueryResult queryResult = post(url, MetricQueryResult.class);
    MetricQueryResult.TimeValue[] data = queryResult.getSeries()[0].getData();

    for (MetricQueryResult.TimeValue point : data) {
      if (point.getValue() != 0) {
        nonZeroPointsCount--;
        expectedSum -= point.getValue();
      }
    }
    Assert.assertEquals(0, nonZeroPointsCount);
    Assert.assertEquals(0, expectedSum);
  }

  private <T> T post(String url, Type type) throws Exception {
    HttpResponse response = doPost(url, null);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return new Gson().fromJson(EntityUtils.toString(response.getEntity()), type);
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

  private void verifySearchResultContains(String url, List<String> expectedValues) throws Exception {
    HttpResponse response = doPost(url, null);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String result = EntityUtils.toString(response.getEntity());
    List<String> reply = new Gson().fromJson(result, new TypeToken<List<String>>() { }.getType());
    Assert.assertTrue(reply.containsAll(expectedValues));
  }

}
