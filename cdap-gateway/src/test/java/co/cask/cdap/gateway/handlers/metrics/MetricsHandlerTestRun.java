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
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Search available contexts and metrics tests
 */
public class MetricsHandlerTestRun extends MetricsSuiteTestBase {

  private static long emitTs;
  private List<String> tagsList;

  @Before
  public void setup() throws Exception {
    setupMetrics();
    tagsList = ImmutableList.of("namespace", "app", "program-type", "program", "flowlet");
  }

  private static void setupMetrics() throws Exception {
    // Adding metrics for app "WordCount1" in namespace "myspace", "WCount1" in "yourspace"
    MetricsCollector collector =
      collectionService.getCollector(getFlowletContext("myspace", "WordCount1", "WordCounter", "run1", "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);
    collector = collectionService.getCollector(getFlowletContext("yourspace", "WCount1", "WordCounter",
                                                                 "run1", "splitter"));
    collector.increment("reads", 1);
    collector = collectionService.getCollector(getFlowletContext("yourspace", "WCount1", "WCounter",
                                                                 "run1", "splitter"));
    emitTs = System.currentTimeMillis();
    // we want to emit in two different seconds
    // todo : figure out why we need this
    TimeUnit.SECONDS.sleep(1);
    collector.increment("reads", 1);
    TimeUnit.MILLISECONDS.sleep(2000);
    collector.increment("reads", 2);

    collector = collectionService.getCollector(getFlowletContext("yourspace", "WCount1", "WCounter",
                                                                 "run1", "counter"));
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
    collector = collectionService.getCollector(getFlowletContext("myspace", "WordCount1", "WordCounter",
                                                                 "run1", "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);

    collector = collectionService.getCollector(getFlowletContext("myspace", "WordCount1", "WordCounter",
                                                                 "run1", "collector"));
    collector.increment("aa", 1);
    collector.increment("zz", 1);
    collector.increment("ab", 1);

    // also: user metrics
    Metrics userMetrics = new ProgramUserMetrics(
      collectionService.getCollector(getFlowletContext("myspace", "WordCount1", "WordCounter",
                                                       "run1", "splitter")));
    userMetrics.count("reads", 1);
    userMetrics.count("writes", 2);

    collector = collectionService.getCollector(ImmutableMap.of(Constants.Metrics.Tag.CLUSTER_METRICS, "true"));
    collector.increment("resources.total.storage", 10);

    // need a better way to do this
    TimeUnit.SECONDS.sleep(2);
  }

  @Test
  public void testInvalidRequests() throws Exception {
    HttpResponse response = doPost("/v3/metrics/search?target=childContext&context=wrongtag.myspace", null);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());

    response = doPost("/v3/metrics/search?target=metric&context=namespace.myspace.app.WCount1.invalid.f", null);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());

    response = doPost("/v3/metrics/query?metric=system.reads&context=namespace.myspace.app.WCount1." +
                        "program_type.f.random.flowName", null);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testSearchContext() throws Exception {
    // empty context
    verifySearchResultContains("/v3/metrics/search?target=childContext",
                               ImmutableList.<String>of("namespace.myspace", "namespace.yourspace"));

    // WordCount is in myspace, WCount in yourspace
    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.myspace",
                       ImmutableList.<String>of("namespace.myspace.app.WordCount1"));
    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace",
                       ImmutableList.<String>of("namespace.yourspace.app.WCount1"));

    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.myspace.app.WordCount1",
                       ImmutableList.<String>of("namespace.myspace.app.WordCount1.program-type.f"));

    // WordCount should be found in myspace, not in yourspace
    verifySearchResult("/v3/metrics/search?target=" +
                         "childContext&context=namespace.myspace.app.WordCount1.program-type.f",
                       ImmutableList.<String>of("namespace.myspace.app.WordCount1.program-type.f.program.WordCounter"));
    verifySearchResult("/v3/metrics/search?target=" +
                         "childContext&context=namespace.yourspace.app.WordCount1.program-type.f",
                       ImmutableList.<String>of());

    // WCount should be found in yourspace, not in myspace
    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace.app.WCount1",
                       ImmutableList.<String>of("namespace.yourspace.app.WCount1.program-type.b",
                                                "namespace.yourspace.app.WCount1.program-type.f",
                                                "namespace.yourspace.app.WCount1.program-type.p"));
    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.myspace.app.WCount1",
                       ImmutableList.<String>of());

    // verify other metrics for WCount app
    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace.app.WCount1" +
                         ".program-type.b.program.ClassicWordCount",
                       ImmutableList.<String>of(
                         "namespace.yourspace.app.WCount1.program-type.b.program.ClassicWordCount.run-id.run1"));
    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace.app.WCount1" +
                         ".program-type.b.program.ClassicWordCount.run-id.run1",
                       ImmutableList.<String>of("namespace.yourspace.app.WCount1.program-type.b.program." +
                                                  "ClassicWordCount.run-id.run1.mr-type.m",
                                                "namespace.yourspace.app.WCount1.program-type.b.program." +
                                                  "ClassicWordCount.run-id.run1.mr-type.r"));
    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace.app.WCount1" +
                         ".program-type.b.program.ClassicWordCount.run-id.run1.mr-type.m",
                       ImmutableList.<String>of("namespace.yourspace.app.WCount1.program-type.b.program." +
                                                  "ClassicWordCount.run-id.run1.mr-type.m.instance-id.task1"));
    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace.app.WCount1" +
                         ".program-type.b.program.ClassicWordCount.run-id.run1.mr-type.m.instance-id.task1",
                       ImmutableList.<String>of());

    // verify "*"
    verifySearchResultContains("/v3/metrics/search?target=childContext&context=namespace.*",
                               ImmutableList.<String>of("namespace.*.app.WordCount1",
                                                        "namespace.*.app.WCount1"));

    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.*.app.*",
                       ImmutableList.<String>of("namespace.*.app.*.program-type.b",
                                                "namespace.*.app.*.program-type.f",
                                                "namespace.*.app.*.program-type.p"));

    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace.app.WCount1.program-type.*",
                       ImmutableList.<String>of("namespace.yourspace.app.WCount1." +
                                                  "program-type.*.program.ClassicWordCount",
                                                "namespace.yourspace.app.WCount1.program-type.*.program.RCounts",
                                                "namespace.yourspace.app.WCount1.program-type.*.program.WCounter",
                                                "namespace.yourspace.app.WCount1.program-type.*.program.WordCounter"));
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

    List<TimeSeriesResult> groupByResult =
      ImmutableList.of(new TimeSeriesResult(ImmutableMap.of("flowlet", "counter"), 1),
                       new TimeSeriesResult(ImmutableMap.of("flowlet", "splitter"), 3));

    verifyGroupByResult(
      "/v3/metrics/query?context=" + getContext("yourspace", "WCount1", "f", "WCounter") +
        "&metric=system.reads&groupBy=" + Constants.Metrics.Tag.FLOWLET + "&start=" + start + "&end="
        + end, groupByResult);

    groupByResult =
      ImmutableList.of(new TimeSeriesResult(ImmutableMap.of("app", "WCount1", "program-type", "b"), 2),
                       new TimeSeriesResult(ImmutableMap.of("app", "WCount1", "program-type", "p"), 1),
                       new TimeSeriesResult(ImmutableMap.of("app", "WCount1", "program-type", "f"), 5));

    verifyGroupByResult(
      "/v3/metrics/query?context=" + getContext("yourspace") +
        "&metric=system.reads&groupBy=" + Constants.Metrics.Tag.APP + "," + Constants.Metrics.Tag.PROGRAM_TYPE +
        "&start=" + start + "&end=" + end, groupByResult);
  }

  private void verifyGroupByResult(String url, List<TimeSeriesResult> groupByResult) throws Exception {
    MetricQueryResult result = post(url, MetricQueryResult.class);
    Assert.assertEquals(groupByResult.size(), result.getSeries().length);
    for (MetricQueryResult.TimeSeries timeSeries : result.getSeries()) {
      for (TimeSeriesResult expectedTs : groupByResult) {
        if (compareTagValues(expectedTs.getTagValues(), timeSeries.getGrouping())) {
          assertTimeValues(expectedTs, timeSeries);
        }
      }
    }
  }

  private boolean compareTagValues(Map<String, String> expected, Map<String, String> actual) {
    for (String key : expected.keySet()) {
      if (!actual.containsKey(key)) {
        return false;
      }
      if (!expected.get(key).equals(actual.get(key))) {
        return false;
      }
    }
    return true;
  }

  private void assertTimeValues(TimeSeriesResult expected, MetricQueryResult.TimeSeries actual) {
    long expectedValue = expected.getTimeSeriesSum();
    for (MetricQueryResult.TimeValue timeValue : actual.getData()) {
      expectedValue -= timeValue.getValue();
    }
    Assert.assertEquals(0, expectedValue);
  }

  private void verifyEmptyQueryResult(String url) throws Exception {
    MetricQueryResult queryResult = post(url, MetricQueryResult.class);
    Assert.assertEquals(0, queryResult.getSeries().length);
  }

  private String getContext(String... tags) {
    String context = "";
    for (int i = 0; i < tags.length; i++) {
      context += tagsList.get(i) + "." + tags[i] + ".";
    }
    return context.substring(0, context.length() - 1);
  }

  @Test
  public void testSearchMetrics() throws Exception {
    // metrics in myspace
    verifySearchResult("/v3/metrics/search?target=metric&context=namespace.myspace.app.WordCount1" +
                         ".program-type.f.program.WordCounter.run-id.run1.flowlet.splitter",
                       ImmutableList.<String>of("system.reads", "system.writes", "user.reads", "user.writes"));

    verifySearchResult("/v3/metrics/search?target=metric&context=namespace.myspace.app.WordCount1" +
                         ".program-type.f.program.WordCounter.run-id.run1.flowlet.collector",
                       ImmutableList.<String>of("system.aa", "system.ab", "system.zz"));

    verifySearchResult("/v3/metrics/search?target=metric&context=namespace.myspace.app.WordCount1" +
                         ".program-type.f.program.WordCounter.run-id.run1",
                       ImmutableList.<String>of("system.aa", "system.ab", "system.reads",
                                                "system.writes", "system.zz", "user.reads", "user.writes"));

    // wrong namespace
    verifySearchResult("/v3/metrics/search?target=metric&context=namespace.yourspace.app.WordCount1." +
                         "program-type.f.program.WordCounter.run-id.run1.flowlet.splitter",
                       ImmutableList.<String>of());


    // metrics in yourspace
    verifySearchResult("/v3/metrics/search?target=metric&context=namespace.yourspace.app.WCount1" +
                         ".program-type.f.program.WCounter.run-id.run1.flowlet.splitter",
                       ImmutableList.<String>of("system.reads"));

    // wrong namespace
    verifySearchResult("/v3/metrics/search?target=metric&context=namespace.myspace.app.WCount1" +
                         ".program-type.f.program.WCounter.run-id.run1.flowlet.splitter",
                       ImmutableList.<String>of());

    // verify "*"
    verifySearchResult("/v3/metrics/search?target=metric&context=namespace.myspace.app.WordCount1" +
                         ".program-type.f.program.WordCounter.run-id.run1.flowlet.*",
                       ImmutableList.<String>of("system.aa", "system.ab", "system.reads",
                                                "system.writes", "system.zz", "user.reads", "user.writes"));
    verifySearchResult("/v3/metrics/search?target=metric&context=namespace.myspace.app.WordCount1" +
                         ".program-type.f.program.*.run-id.run1",
                       ImmutableList.<String>of("system.aa", "system.ab", "system.reads",
                                                "system.writes", "system.zz", "user.reads", "user.writes"));
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

  class TimeSeriesResult {
    public Map<String, String> getTagValues() {
      return tagValues;
    }

    public long getTimeSeriesSum() {
      return timeSeriesSum;
    }

    Map<String, String> tagValues;
    long timeSeriesSum;
    TimeSeriesResult(Map<String, String> tagValues, long timeSeriesSum) {
      this.tagValues = tagValues;
      this.timeSeriesSum = timeSeriesSum;
    }
  }

}
