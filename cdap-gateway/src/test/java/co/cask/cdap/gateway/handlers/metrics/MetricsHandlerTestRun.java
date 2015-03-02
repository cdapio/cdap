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
import co.cask.cdap.common.conf.Constants.Metrics.Tag;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Search available contexts and metrics tests
 */
public class MetricsHandlerTestRun extends MetricsSuiteTestBase {
  // for testing dots in tag names
  private static final String DOT_NAMESPACE = Tag.NAMESPACE + ".my.namespace." + Tag.NAMESPACE;
  private static final String DOT_APP = Tag.APP + ".my.app." + Tag.APP;
  private static final String DOT_FLOW = Tag.FLOW + ".my.flow." + Tag.FLOW;
  private static final String DOT_RUN = Tag.RUN_ID + ".my.run." + Tag.RUN_ID;
  private static final String DOT_FLOWLET = Tag.FLOWLET + ".my.flowlet." + Tag.FLOWLET;

  private static final String DOT_NAMESPACE_ESCAPED = DOT_NAMESPACE.replaceAll("\\.", "~");
  private static final String DOT_APP_ESCAPED = DOT_APP.replaceAll("\\.", "~");
  private static final String DOT_FLOW_ESCAPED = DOT_FLOW.replaceAll("\\.", "~");
  private static final String DOT_RUN_ESCAPED = DOT_RUN.replaceAll("\\.", "~");
  private static final String DOT_FLOWLET_ESCAPED = DOT_FLOWLET.replaceAll("\\.", "~");

  private static final List<String> FLOW_TAGS = ImmutableList.of(Tag.NAMESPACE, Tag.APP, Tag.FLOW, Tag.FLOWLET);

  private static long emitTs;

  @Before
  public void setup() throws Exception {
    setupMetrics();
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

    collector = collectionService.getCollector(new HashMap<String, String>());
    collector.increment("resources.total.storage", 10);

    // dots in tag values
    collector = collectionService.getCollector(getFlowletContext(DOT_NAMESPACE,
                                                                 DOT_APP,
                                                                 DOT_FLOW,
                                                                 DOT_RUN,
                                                                 DOT_FLOWLET));
    collector.increment("dot.reads", 55);


    // need a better way to do this
    TimeUnit.SECONDS.sleep(2);
  }

  @Test
  public void testSearchContext() throws Exception {
    // empty context
    verifySearchResultContains("/v3/metrics/search?target=childContext",
                               ImmutableList.<String>of("namespace." + DOT_NAMESPACE_ESCAPED,
                                                        "namespace.myspace", "namespace.yourspace"));

    // WordCount is in myspace, WCount in yourspace
    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.myspace",
                       ImmutableList.<String>of("namespace.myspace.app.WordCount1"));
    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace",
                       ImmutableList.<String>of("namespace.yourspace.app.WCount1"));

    // WordCount should be found in myspace, not in yourspace
    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.myspace.app.WordCount1",
                       ImmutableList.<String>of("namespace.myspace.app.WordCount1.flow.WordCounter"));
    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace.app.WordCount1",
                       ImmutableList.<String>of());

    // WCount should be found in yourspace, not in myspace
    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace.app.WCount1",
                       ImmutableList.<String>of("namespace.yourspace.app.WCount1.flow.WCounter",
                                                "namespace.yourspace.app.WCount1.flow.WordCounter",
                                                "namespace.yourspace.app.WCount1.mapreduce.ClassicWordCount",
                                                "namespace.yourspace.app.WCount1.procedure.RCounts"));

    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.myspace.app.WCount1",
                       ImmutableList.<String>of());

    // verify other metrics for WCount app
    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace.app.WCount1" +
                         ".mapreduce.ClassicWordCount",
                       ImmutableList.<String>of("namespace.yourspace.app.WCount1" +
                                                  ".mapreduce.ClassicWordCount.dataset.*"));

    // verify other metrics for WCount app
    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace.app.WCount1" +
                         ".mapreduce.ClassicWordCount.dataset.*",
                       ImmutableList.<String>of("namespace.yourspace.app.WCount1" +
                                                  ".mapreduce.ClassicWordCount.dataset.*.run.run1"));

    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace.app.WCount1" +
                         ".mapreduce.ClassicWordCount.dataset.*.run.run1",
                       ImmutableList.<String>of("namespace.yourspace.app.WCount1" +
                                                  ".mapreduce.ClassicWordCount.dataset.*.run.run1.tasktype.m",
                                                "namespace.yourspace.app.WCount1" +
                                                  ".mapreduce.ClassicWordCount.dataset.*.run.run1.tasktype.r"));

    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace.app.WCount1" +
                         ".mapreduce.ClassicWordCount.dataset.*.run.run1.tasktype.m",
                       ImmutableList.<String>of("namespace.yourspace.app.WCount1.mapreduce.ClassicWordCount" +
                                                  ".dataset.*.run.run1.tasktype.m.instance.task1"));

    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace.app.WCount1" +
                         ".mapreduce.ClassicWordCount.dataset.*.run.run1.tasktype.m.instance.task1",
                       ImmutableList.<String>of());

    // verify "*"
    verifySearchResultContains("/v3/metrics/search?target=childContext&context=namespace.*",
                               ImmutableList.<String>of("namespace.*.app.WordCount1",
                                                        "namespace.*.app.WCount1"));

    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.*.app.*",
                       ImmutableList.<String>of("namespace.*.app.*.flow.WCounter",
                                                "namespace.*.app.*.flow.WordCounter",
                                                "namespace.*.app.*.flow." + DOT_FLOW_ESCAPED,
                                                "namespace.*.app.*.mapreduce.ClassicWordCount",
                                                "namespace.*.app.*.procedure.RCounts"));

    verifySearchResult("/v3/metrics/search?target=childContext&context=namespace.yourspace.app.*.flow.WCounter",
                       ImmutableList.<String>of("namespace.yourspace.app.*.flow.WCounter.dataset.*"));

    // verify dots more
    String parts[] = new String[] {
      "namespace." + DOT_NAMESPACE_ESCAPED,
      "app." + DOT_APP_ESCAPED,
      "flow." + DOT_FLOW_ESCAPED,
      "dataset.*",
      "run." + DOT_RUN_ESCAPED,
      "flowlet." + DOT_FLOWLET_ESCAPED
    };
    // drilling down into above parts one at a time
    String context = parts[0];
    int i = 1;
    do {
      String contextNext = context + "." + parts[i];
      verifySearchResult("/v3/metrics/search?target=childContext&context=" + context,
                         ImmutableList.<String>of(contextNext));
      context = contextNext;
      i++;
    } while (i < parts.length);
  }


  @Test
  public void testQueryMetrics() throws Exception {

    //aggregate result, in the right namespace
    verifyAggregateQueryResult(
      "/v3/metrics/query?context=" + getContext("yourspace", "WCount1", "WCounter", "splitter") +
        "&metric=system.reads&aggregate=true", 3);
    verifyAggregateQueryResult(
      "/v3/metrics/query?context=" + getContext("yourspace", "WCount1", "WCounter", "counter") +
        "&metric=system.reads&aggregate=true", 1);

    verifyAggregateQueryResult(
      "/v3/metrics/query?context=" + getContext("yourspace", "WCount1", "WCounter", "*") +
        "&metric=system.reads&aggregate=true", 4);

    // aggregate result, in the wrong namespace
    verifyEmptyQueryResult(
      "/v3/metrics/query?context=" + getContext("myspace", "WCount1", "WCounter", "splitter") +
        "&metric=system.reads&aggregate=true");

    verifyAggregateQueryResult("/v3/metrics/query?context=" +
                                 getContext(DOT_NAMESPACE_ESCAPED, DOT_APP_ESCAPED,
                                            DOT_FLOW_ESCAPED, DOT_FLOWLET_ESCAPED) +
        "&metric=system.dot.reads&aggregate=true", 55);

    // time range
    // now-60s, now+60s
    verifyRangeQueryResult(
      "/v3/metrics/query?context=" + getContext("yourspace", "WCount1", "WCounter", "splitter") +
        "&metric=system.reads&start=now%2D60s&end=now%2B60s", 2, 3);

    // note: times are in seconds, hence "divide by 1000";
    long start = (emitTs - 60 * 1000) / 1000;
    long end = (emitTs + 60 * 1000) / 1000;
    verifyRangeQueryResult(
      "/v3/metrics/query?context=" + getContext("yourspace", "WCount1", "WCounter", "splitter") +
        "&metric=system.reads&start=" + start + "&end="
        + end, 2, 3);
    // range query, in the wrong namespace
    verifyEmptyQueryResult(
      "/v3/metrics/query?context=" + getContext("myspace", "WCount1", "WCounter", "splitter") +
        "&metric=system.reads&start=" + start + "&end="
        + end);

    List<TimeSeriesResult> groupByResult =
      ImmutableList.of(new TimeSeriesResult(ImmutableMap.of("flowlet", "counter"), 1),
                       new TimeSeriesResult(ImmutableMap.of("flowlet", "splitter"), 3));

    verifyGroupByResult(
      "/v3/metrics/query?context=" + getContext("yourspace", "WCount1", "WCounter") +
        "&metric=system.reads&groupBy=" + Tag.FLOWLET + "&start=" + start + "&end="
        + end, groupByResult);

    groupByResult =
      ImmutableList.of(new TimeSeriesResult(ImmutableMap.of("ns", "myspace", "flowlet", "splitter"), 2),
                       new TimeSeriesResult(ImmutableMap.of("ns", "yourspace", "flowlet", "counter"), 1),
                       new TimeSeriesResult(ImmutableMap.of("ns", "yourspace", "flowlet", "splitter"), 4));

    verifyGroupByResult(
      "/v3/metrics/query?metric=system.reads" +
        "&groupBy=" + Tag.NAMESPACE + "," + Tag.FLOWLET +
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
      context += FLOW_TAGS.get(i) + "." + tags[i] + ".";
    }
    return context.substring(0, context.length() - 1);
  }

  @Test
  public void testSearchMetrics() throws Exception {

    // verify dots
    verifySearchMetricResult("/v3/metrics/search?target=metric&context=" +
                              Tag.NAMESPACE + "." + DOT_NAMESPACE_ESCAPED + "." +
                              Tag.APP + "." + DOT_APP_ESCAPED + "." +
                              Tag.FLOW + "." + DOT_FLOW_ESCAPED + "." +
                              Tag.DATASET + ".*." +
                              Tag.RUN_ID + "." + DOT_RUN_ESCAPED + "." +
                              Tag.FLOWLET + "." + DOT_FLOWLET_ESCAPED,
                             ImmutableList.<String>of("system.dot.reads"));
    // metrics in myspace
    verifySearchMetricResult("/v3/metrics/search?target=metric&context=namespace.myspace.app.WordCount1" +
                         ".flow.WordCounter.dataset.*.run.run1.flowlet.splitter",
                       ImmutableList.<String>of("system.reads", "system.writes", "user.reads", "user.writes"));

    verifySearchMetricResult("/v3/metrics/search?target=metric&context=namespace.myspace.app.WordCount1" +
                         ".flow.WordCounter.dataset.*.run.run1.flowlet.collector",
                       ImmutableList.<String>of("system.aa", "system.ab", "system.zz"));

    verifySearchMetricResult("/v3/metrics/search?target=metric&context=namespace.myspace.app.WordCount1" +
                         ".flow.WordCounter.dataset.*.run.run1",
                       ImmutableList.<String>of("system.aa", "system.ab", "system.reads",
                                                "system.writes", "system.zz", "user.reads", "user.writes"));

    // wrong namespace
    verifySearchMetricResult("/v3/metrics/search?target=metric&context=namespace.yourspace.app.WordCount1." +
                         "f.WordCounter.dataset.*.run.run1.flowlet.splitter",
                       ImmutableList.<String>of());


    // metrics in yourspace
    verifySearchMetricResult("/v3/metrics/search?target=metric&context=namespace.yourspace.app.WCount1" +
                               ".flow.WCounter.dataset.*.run.run1.flowlet.splitter",
                             ImmutableList.<String>of("system.reads"));

    // wrong namespace
    verifySearchMetricResult("/v3/metrics/search?target=metric&context=namespace.myspace.app.WCount1" +
                               ".flow.WCounter.dataset.*.run.run1.flowlet.splitter",
                             ImmutableList.<String>of());

    // verify "*"
    verifySearchMetricResult("/v3/metrics/search?target=metric&context=namespace.myspace.app.WordCount1" +
                               ".flow.WordCounter.dataset.*.run.run1.flowlet.*",
                             ImmutableList.<String>of("system.aa", "system.ab", "system.reads",
                                                      "system.writes", "system.zz", "user.reads", "user.writes"));
    verifySearchMetricResult("/v3/metrics/search?target=metric&context=namespace.myspace.app.WordCount1" +
                               ".flow.*.dataset.*.run.run1",
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
    // We want to make sure expectedValues are in the response. Response may also have other things that denote
    // null values for tags - we'll ignore them.
    Assert.assertTrue(reply.containsAll(expectedValues));
    for (String returned: reply) {
      Assert.assertTrue(expectedValues.contains(returned) || returned.endsWith(".*"));
    }
  }

  private void verifySearchMetricResult(String url, List<String> expectedValues) throws Exception {
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
