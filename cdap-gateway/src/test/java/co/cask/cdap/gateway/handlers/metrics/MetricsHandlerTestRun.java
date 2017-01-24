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

import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.proto.MetricQueryResult;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
  private static final Gson GSON = new Gson();

  private static final List<String> FLOW_TAGS_HUMAN = ImmutableList.of("namespace", "app", "flow", "flowlet");

  private static long emitTs;

  @Before
  public void setup() throws Exception {
    setupMetrics();
  }

  private static void setupMetrics() throws Exception {
    // Adding metrics for app "WordCount1" in namespace "myspace", "WCount1" in "yourspace"
    MetricsContext collector =
      collectionService.getContext(getFlowletContext("myspace", "WordCount1", "WordCounter", "run1", "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);
    collector = collectionService.getContext(getFlowletContext("yourspace", "WCount1", "WordCounter",
                                                               "run1", "splitter"));
    collector.increment("reads", 1);
    collector = collectionService.getContext(getFlowletContext("yourspace", "WCount1", "WCounter",
                                                               "run1", "splitter"));
    emitTs = System.currentTimeMillis();
    // we want to emit in two different seconds
    // todo : figure out why we need this
    TimeUnit.SECONDS.sleep(1);
    collector.increment("reads", 1);
    TimeUnit.MILLISECONDS.sleep(2000);
    collector.increment("reads", 2);

    collector = collectionService.getContext(getFlowletContext("yourspace", "WCount1", "WCounter",
                                                               "run1", "counter"));
    collector.increment("reads", 1);
    collector = collectionService.getContext(getMapReduceTaskContext("yourspace", "WCount1", "ClassicWordCount",
                                                                     MapReduceMetrics.TaskType.Mapper,
                                                                     "run1", "task1"));
    collector.increment("reads", 1);
    collector = collectionService.getContext(
      getMapReduceTaskContext("yourspace", "WCount1", "ClassicWordCount",
                              MapReduceMetrics.TaskType.Reducer, "run1", "task2"));
    collector.increment("reads", 1);
    collector = collectionService.getContext(getFlowletContext("myspace", "WordCount1", "WordCounter",
                                                               "run1", "splitter"));
    collector.increment("reads", 1);
    collector.increment("writes", 1);

    collector = collectionService.getContext(getFlowletContext("myspace", "WordCount1", "WordCounter",
                                                               "run1", "collector"));
    collector.increment("aa", 1);
    collector.increment("zz", 1);
    collector.increment("ab", 1);

    collector = collectionService.getContext(getWorkerContext("yourspace", "WCount1", "WorkerWordCount",
                                                              "run1", "task1"));

    collector.increment("workerreads", 5);
    collector.increment("workerwrites", 6);

    collector = collectionService.getContext(getWorkerContext("yourspace", "WCount1", "WorkerWordCount",
                                                              "run2", "task1"));

    collector.increment("workerreads", 5);
    collector.increment("workerwrites", 6);

    // also: user metrics
    Metrics userMetrics = new ProgramUserMetrics(
      collectionService.getContext(getFlowletContext("myspace", "WordCount1", "WordCounter",
                                                     "run1", "splitter")));
    userMetrics.count("reads", 1);
    userMetrics.count("writes", 2);

    collector = collectionService.getContext(new HashMap<String, String>());
    collector.increment("resources.total.storage", 10);

    // need a better way to do this
    TimeUnit.SECONDS.sleep(2);
  }

  private List<Map<String, String>> getSearchResultExpected(String... searchExpectations) {
    // iterate search results, add them as name-value map to the result list and return the list
    List<Map<String, String>> result = Lists.newArrayList();
    for (int i = 0; i < searchExpectations.length; i += 2) {
      result.add(ImmutableMap.of("name", searchExpectations[i], "value", searchExpectations[i + 1]));
    }
    return result;
  }

  @Test
  public void testSearchWithTags() throws Exception {
    // empty context
    verifySearchResultWithTags("/v3/metrics/search?target=tag", getSearchResultExpected("namespace", "myspace",
                                                                                        "namespace", "yourspace",
                                                                                        "namespace", "system"));

    // WordCount is in myspace, WCount in yourspace
    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:myspace",
                       getSearchResultExpected("app", "WordCount1"));
    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:yourspace",
                       getSearchResultExpected("app", "WCount1"));

    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:yourspace",
                               getSearchResultExpected("app", "WCount1"));

    // WordCount should be found in myspace, not in yourspace
    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:myspace&tag=app:WordCount1",
                       getSearchResultExpected("flow", "WordCounter"));

    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:yourspace&tag=app:WordCount1",
                               getSearchResultExpected());

    // WCount should be found in yourspace, not in myspace
    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:yourspace&tag=app:WCount1",
                       getSearchResultExpected("flow", "WCounter",
                                               "flow", "WordCounter",
                                               "mapreduce", "ClassicWordCount",
                                               "worker", "WorkerWordCount"
                       ));

    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:myspace&tag=app:WCount1",
                               getSearchResultExpected());

    // verify other metrics for WCount app
    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:yourspace&tag=app:WCount1" +
                                 "&tag=mapreduce:ClassicWordCount",
                               getSearchResultExpected("run", "run1"));

    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:yourspace&tag=app:WCount1" +
                                 "&tag=mapreduce:ClassicWordCount&tag=run:run1",
                               getSearchResultExpected("tasktype", "m", "tasktype", "r"));

    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:yourspace&tag=app:WCount1" +
                                 "&tag=mapreduce:ClassicWordCount&tag=run:run1&tag=tasktype:m",
                               getSearchResultExpected("instance", "task1"));

    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:yourspace&tag=app:WCount1" +
                                 "&tag=mapreduce:ClassicWordCount&tag=run:run1&tag=tasktype:m&tag=instance:task1",
                               getSearchResultExpected());

    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:yourspace&tag=app:WCount1" +
                                 "&tag=worker:WorkerWordCount",
                               getSearchResultExpected("run", "run1",
                                                       "run", "run2"));
    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:yourspace&tag=app:WCount1" +
                                 "&tag=worker:WorkerWordCount&tag=run:run1",
                               getSearchResultExpected("instance", "task1"));

    // verify "*"

    // components (services) are listed as optional, as metrics are cleared between tests,
    // some of the services might not have emitted metrics yet since the beginning of this test,
    // so all expected component metrics are listed under optional.
    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:*",
                               //required
                               getSearchResultExpected("app", "WordCount1",
                                                       "app", "WCount1"),
                               //optional
                               getSearchResultExpected(
                                 "app", "WordCount1",
                                 "app", "WCount1",
                                 "component", "metrics",
                                 "component", "dataset.service",
                                 "component", "dataset.executor",
                                 "component", "transactions",
                                 "component", "metrics.processor"));

    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:*&tag=app:*",
                               getSearchResultExpected("flow", "WCounter",
                                                       "flow", "WordCounter",
                                                       "mapreduce", "ClassicWordCount",
                                                       "worker", "WorkerWordCount"));

    verifySearchResultWithTags("/v3/metrics/search?target=tag&tag=namespace:*&tag=app:*&tag=flow:*",
                               getSearchResultExpected("run", "run1"));
  }

  @Test
  public void testAggregateQueryBatch() throws Exception {

    QueryRequestFormat query1 = new QueryRequestFormat(getTagsMap("namespace", "yourspace", "app", "WCount1",
                                                                  "flow", "WCounter", "flowlet", "splitter"),
                                           ImmutableList.of("system.reads"), ImmutableList.<String>of(),
                                           ImmutableMap.<String, String>of());

    // empty time range should default to aggregate=true
    QueryRequestFormat query2 = new QueryRequestFormat(getTagsMap("namespace", "yourspace", "app", "WCount1",
                                                                  "flow", "WCounter", "flowlet", "counter"),
                                           ImmutableList.of("system.reads"),
                                           ImmutableList.<String>of(),
                                           ImmutableMap.of("aggregate", "true"));


    QueryRequestFormat query3 = new QueryRequestFormat(getTagsMap("namespace", "yourspace", "app", "WCount1",
                                                                  "flow", "WCounter", "flowlet", "*"),
                                           ImmutableList.of("system.reads"),
                                           ImmutableList.<String>of(),
                                           ImmutableMap.of("aggregate", "true"));


    QueryRequestFormat query4 = new QueryRequestFormat(ImmutableMap.of("namespace", "myspace", "app", "WordCount1",
                                                                       "flow", "WordCounter", "flowlet", "splitter"),
                                           ImmutableList.of("system.reads", "system.writes"),
                                           ImmutableList.<String>of(),
                                           ImmutableMap.of("aggregate", "true"));

    // test batching of multiple queries

    String resolution = Integer.MAX_VALUE + "s";

    ImmutableMap<String, QueryResult> expected =
      ImmutableMap.of("testQuery1",
                      new QueryResult(ImmutableList.of(new TimeSeriesSummary(
                        ImmutableMap.<String, String>of(), "system.reads", 1, 3)), resolution),
                      "testQuery2",
                      new QueryResult(ImmutableList.of(new TimeSeriesSummary(
                          ImmutableMap.<String, String>of(), "system.reads", 1, 1)), resolution));

    batchTest(ImmutableMap.of("testQuery1", query1, "testQuery2", query2), expected);

    // test batching of multiple queries, with one query having multiple metrics to query

    expected = ImmutableMap.of("testQuery3",
                               new QueryResult(ImmutableList.of(
                                 new TimeSeriesSummary(ImmutableMap.<String, String>of(), "system.reads", 1, 4))
                                 , resolution),
                               "testQuery4",
                               new QueryResult(ImmutableList.of(
                                 new TimeSeriesSummary(ImmutableMap.<String, String>of(), "system.reads", 1, 2),
                                                new TimeSeriesSummary(ImmutableMap.<String, String>of(),
                                                                "system.writes", 1, 2)), resolution)
    );

    batchTest(ImmutableMap.of("testQuery3", query3, "testQuery4", query4), expected);
  }

  @Test
  public void testInvalidRequest() throws Exception {
    // test invalid request - query without any query Params and body content
    HttpResponse response = doPost("/v3/metrics/query", null);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());

    // batch query with empty metrics list
    QueryRequestFormat invalidQuery = new QueryRequestFormat(
      ImmutableMap.of("namespace", "myspace", "app", "WordCount1", "flow", "WordCounter", "flowlet", "splitter"),
      ImmutableList.<String>of(), ImmutableList.<String>of(), ImmutableMap.of("aggregate", "true"));

    response = doPost("/v3/metrics/query", GSON.toJson(ImmutableMap.of("invalid", invalidQuery)));
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());

    // test invalid request - query without any metric Params
    response = doPost("/v3/metrics/query?context=namespace.default", null);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    response = doPost("/v3/metrics/query?tag=namespace.default", null);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
  }

  private Map<String, String> getTagsMap(String... entries) {
    Map<String, String> tagsMap = Maps.newHashMap();
    for (int i = 0; i < entries.length; i += 2) {
      tagsMap.put(entries[i], entries[i + 1]);
    }
    return tagsMap;
  }


  /**
   * Helper class to construct json for MetricQueryRequest for batch queries
   */
  private class QueryRequestFormat {
    Map<String, String> tags;
    List<String> metrics;
    List<String> groupBy;
    Map<String, String> timeRange;

    QueryRequestFormat(Map<String, String> tags, List<String> metrics, List<String> groupBy,
                       Map<String, String> timeRange) {
      this.tags = tags;
      this.metrics = metrics;
      this.groupBy = groupBy;
      this.timeRange = timeRange;
    }
  }

  private class QueryResult {
    private ImmutableList<TimeSeriesSummary> expectedList;
    private String expectedResolution;

    public QueryResult(ImmutableList<TimeSeriesSummary> list, String resolution) {
      expectedList = list;
      expectedResolution = resolution;
    }

    public String getExpectedResolution() {
      return expectedResolution;
    }

    public ImmutableList<TimeSeriesSummary> getExpectedList() {
      return expectedList;
    }

  }

  @Test
  public void testTimeRangeQueryBatch() throws Exception {
    // note: times are in seconds, hence "divide by 1000";
    long start = (emitTs - 60 * 1000) / 1000;
    long end = (emitTs + 60 * 1000) / 1000;
    int count = 120;

    QueryRequestFormat query1 = new QueryRequestFormat(getTagsMap("namespace", "yourspace", "app", "WCount1",
                                                      "flow", "WCounter", "flowlet", "splitter"),
                                           ImmutableList.of("system.reads"), ImmutableList.<String>of(),
                                           ImmutableMap.of("start", String.valueOf(start),
                                                           "end", String.valueOf(end)));

    QueryRequestFormat query2 = new QueryRequestFormat(getTagsMap("namespace", "yourspace", "app", "WCount1",
                                                      "flow", "WCounter"), ImmutableList.of("system.reads"),
                                           ImmutableList.of("flowlet"),
                                           ImmutableMap.of("start", String.valueOf(start),
                                                           "count", String.valueOf(count)));

    ImmutableMap<String, QueryResult> expected =
      ImmutableMap.of("timeRangeQuery1",
                      new QueryResult(ImmutableList.of(new TimeSeriesSummary(ImmutableMap.<String,
                        String>of(), "system.reads", 2, 3)), "1s"),
                      "timeRangeQuery2",
                      new QueryResult(ImmutableList.of(
                        new TimeSeriesSummary(ImmutableMap.of("flowlet", "counter"), "system.reads", 1, 1),
                        new TimeSeriesSummary(ImmutableMap.of("flowlet", "splitter"), "system.reads", 2, 3)), "1s"));

    Map<String, QueryRequestFormat> batchQueries = ImmutableMap.of("timeRangeQuery1", query1,
                                                                   "timeRangeQuery2", query2);
    batchTest(batchQueries, expected);
  }

  @Test
  public void testMultipleMetricsSingleContext() throws Exception {
    verifyAggregateQueryResult(
      "/v3/metrics/query?tag=namespace:myspace&tag=app:WordCount1&tag=flow:WordCounter&tag=flowlet:splitter" +
        "&metric=system.reads&metric=system.writes&aggregate=true", ImmutableList.of(2L, 2L));

    long start = (emitTs - 60 * 1000) / 1000;
    long end = (emitTs + 300 * 1000) / 1000;
    verifyRangeQueryResult(
      "/v3/metrics/query?tag=namespace:myspace&tag=app:WordCount1&tag=flow:WordCounter&tag=flowlet:collector" +
        "&metric=system.aa&metric=system.ab&metric=system.zz&start=" + start + "&end="
        + end, ImmutableList.of(1L, 1L, 1L), ImmutableList.of(1L, 1L, 1L));
  }

  @Test
  public void testQueryMetricsWithTags() throws Exception {
    //aggregate result, in the right namespace
    verifyAggregateQueryResult(
      "/v3/metrics/query?" + getTags("yourspace", "WCount1", "WCounter", "splitter") +
        "&metric=system.reads&aggregate=true", 3);
    verifyAggregateQueryResult(
      "/v3/metrics/query?" + getTags("yourspace", "WCount1", "WCounter", "counter") +
        "&metric=system.reads&aggregate=true", 1);

    verifyAggregateQueryResult(
      "/v3/metrics/query?" + getTags("yourspace", "WCount1", "WCounter", "*") +
        "&metric=system.reads&aggregate=true", 4);

    verifyAggregateQueryResult(
      "/v3/metrics/query?" + getTags("yourspace", "WCount1", "WCounter") +
        "&metric=system.reads&aggregate=true", 4);

    // aggregate result, in the wrong namespace
    verifyEmptyQueryResult(
      "/v3/metrics/query?" + getTags("myspace", "WCount1", "WCounter", "splitter") +
        "&metric=system.reads&aggregate=true");

    // time range
    // now-60s, now+60s
    verifyRangeQueryResult(
      "/v3/metrics/query?" + getTags("yourspace", "WCount1", "WCounter", "splitter") +
        "&metric=system.reads&start=now%2D60s&end=now%2B60s", 2, 3);

    // note: times are in seconds, hence "divide by 1000";
    long start = (emitTs - 60 * 1000) / 1000;
    long end = (emitTs + 60 * 1000) / 1000;
    verifyRangeQueryResult(
      "/v3/metrics/query?" + getTags("yourspace", "WCount1", "WCounter", "splitter") +
        "&metric=system.reads&start=" + start + "&end="
        + end, 2, 3);
    // range query, in the wrong namespace
    verifyEmptyQueryResult(
      "/v3/metrics/query?" + getTags("myspace", "WCount1", "WCounter", "splitter") +
        "&metric=system.reads&start=" + start + "&end="
        + end);

    List<TimeSeriesResult> groupByResult =
      ImmutableList.of(new TimeSeriesResult(ImmutableMap.of("flowlet", "counter"), 1),
                       new TimeSeriesResult(ImmutableMap.of("flowlet", "splitter"), 3));

    verifyGroupByResult(
      "/v3/metrics/query?" + getTags("yourspace", "WCount1", "WCounter") +
        "&metric=system.reads&groupBy=flowlet&start=" + start + "&end="
        + end, groupByResult);

    groupByResult =
      ImmutableList.of(new TimeSeriesResult(ImmutableMap.of("namespace", "myspace", "flowlet", "splitter"), 2),
                       new TimeSeriesResult(ImmutableMap.of("namespace", "yourspace", "flowlet", "counter"), 1),
                       new TimeSeriesResult(ImmutableMap.of("namespace", "yourspace", "flowlet", "splitter"), 4));

    verifyGroupByResult(
      "/v3/metrics/query?metric=system.reads" +
        "&groupBy=namespace&groupBy=flowlet&start=" + start + "&end=" + end, groupByResult);
  }

  @Test
  public void testInterpolate() throws Exception {
    long start = System.currentTimeMillis() / 1000;
    long end = start + 3;
    Map<String, String> sliceBy = getFlowletContext("interspace", "WordCount1", "WordCounter", "run1", "splitter");
    MetricValues value =
      new MetricValues(sliceBy, "reads", start, 100, MetricType.COUNTER);
    metricStore.add(value);

    value =
      new MetricValues(sliceBy, "reads", end, 400, MetricType.COUNTER);
    metricStore.add(value);

    verifyRangeQueryResult(
      "/v3/metrics/query?" + getTags("interspace", "WordCount1", "WordCounter", "splitter") +
        "&metric=system.reads&interpolate=step&start=" + start + "&end="
        + end, 4, 700);

    verifyRangeQueryResult(
      "/v3/metrics/query?" + getTags("interspace", "WordCount1", "WordCounter", "splitter") +
        "&metric=system.reads&interpolate=linear&start=" + start + "&end="
        + end, 4, 1000);

    // delete the added metrics for testing interpolator
    MetricDeleteQuery deleteQuery = new MetricDeleteQuery(start, end, sliceBy);
    metricStore.delete(deleteQuery);
  }

  @Test
  public void testResolutionInResponse() throws Exception {
    long start = 1;

    String url = "/v3/metrics/query?" + getTags("resolutions", "WordCount1", "WordCounter", "splitter") +
      "&metric=system.reads&resolution=auto&start=" + (start - 1) + "&end="
      + (start + 36000);
    MetricQueryResult queryResult = post(url, MetricQueryResult.class);
    Assert.assertEquals("3600s", queryResult.getResolution());

    url = "/v3/metrics/query?" + getTags("resolutions", "WordCount1", "WordCounter", "splitter") +
      "&metric=system.reads&resolution=1m&start=" + (start - 1) + "&end="
      + (start + 36000);
    queryResult = post(url, MetricQueryResult.class);
    Assert.assertEquals("60s", queryResult.getResolution());

    // Have an aggregate query and ensure that its resolution is INT_MAX
    url = "/v3/metrics/query?" + getTags("WordCount1", "WordCounter", "splitter") +
      "&metric=system.reads";
    queryResult = post(url, MetricQueryResult.class);
    Assert.assertEquals(Integer.MAX_VALUE + "s", queryResult.getResolution());

  }


  @Test
  public void testAutoResolutions() throws Exception {
    long start = 1;
    Map<String, String> sliceBy = getFlowletContext("resolutions", "WordCount1", "WordCounter", "run1", "splitter");

    // 1 second
    metricStore.add(new MetricValues(sliceBy, "reads", start, 1, MetricType.COUNTER));
    metricStore.add(new MetricValues(sliceBy, "reads", start + 1, 1, MetricType.COUNTER));
    metricStore.add(new MetricValues(sliceBy, "reads", start + 2, 1, MetricType.COUNTER));
    // 30 second
    metricStore.add(new MetricValues(sliceBy, "reads", start + 30, 1, MetricType.COUNTER));
    metricStore.add(new MetricValues(sliceBy, "reads", start + 31, 1, MetricType.COUNTER));
    metricStore.add(new MetricValues(sliceBy, "reads", start + 32, 1, MetricType.COUNTER));
    // 1 minute
    metricStore.add(new MetricValues(sliceBy, "reads", start + 60, 1, MetricType.COUNTER));
    // 10 minutes
    metricStore.add(new MetricValues(sliceBy, "reads", start + 600, 1, MetricType.COUNTER));
    // 1 hour
    metricStore.add(new MetricValues(sliceBy, "reads", start + 3600, 1, MetricType.COUNTER));
    // 10 hour
    metricStore.add(new MetricValues(sliceBy, "reads", start + 36000, 1, MetricType.COUNTER));

    // seconds
    verifyRangeQueryResult(
      "/v3/metrics/query?" + getTags("resolutions", "WordCount1", "WordCounter", "splitter") +
        "&metric=system.reads&resolution=auto&start=" + start  + "&end="
        + (start + 600), 4, 4);

    // minutes
    verifyRangeQueryResult(
      "/v3/metrics/query?" + getTags("resolutions", "WordCount1", "WordCounter", "splitter") +
        "&metric=system.reads&resolution=auto&start=" + (start - 1) + "&end="
        + (start + 600), 3, 4);

    // minutes
    verifyRangeQueryResult(
      "/v3/metrics/query?" + getTags("resolutions", "WordCount1", "WordCounter", "splitter") +
        "&metric=system.reads&resolution=auto&start=" + (start - 1) + "&end="
        + (start + 3600), 4, 5);

    // hours
    verifyRangeQueryResult(
      "/v3/metrics/query?" + getTags("resolutions", "WordCount1", "WordCounter", "splitter") +
        "&metric=system.reads&resolution=auto&start=" + (start - 1) + "&end="
        + (start + 36000), 3, 6);

    // delete the added metrics for testing auto resolutions
    MetricDeleteQuery deleteQuery = new MetricDeleteQuery(start, (start + 36000), sliceBy);
    metricStore.delete(deleteQuery);
  }

  private void verifyGroupByResult(String url, List<TimeSeriesResult> groupByResult) throws Exception {
    MetricQueryResult result = post(url, MetricQueryResult.class);
    Assert.assertEquals(groupByResult.size(), result.getSeries().length);
    for (MetricQueryResult.TimeSeries timeSeries : result.getSeries()) {
      boolean timeSeriesMatchFound = false;
      for (TimeSeriesResult expectedTs : groupByResult) {
        if (expectedTs.getTagValues().equals(ImmutableMap.copyOf(timeSeries.getGrouping()))) {
          assertTimeValues(expectedTs, timeSeries);
          timeSeriesMatchFound = true;
        }
      }
      Assert.assertTrue(timeSeriesMatchFound);
    }
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

  private String getTags(String... tags) {
    String result = "";
    for (int i = 0; i < tags.length; i++) {
      result += "&tag=" + FLOW_TAGS_HUMAN.get(i) + ":" + tags[i];
    }
    return result;
  }

  @Test
  public void testSearchMetricsWithTags() throws Exception {
    // metrics in myspace
    verifySearchMetricResult("/v3/metrics/search?target=metric&tag=namespace:myspace&tag=app:WordCount1" +
                               "&tag=flow:WordCounter&tag=dataset:*&tag=run:run1&tag=flowlet:splitter",
                             ImmutableList.of("system.reads", "system.writes", "user.reads", "user.writes"));

    verifySearchMetricResult("/v3/metrics/search?target=metric&tag=namespace:myspace&tag=app:WordCount1" +
                               "&tag=flow:WordCounter&tag=dataset:*&tag=run:run1&tag=flowlet:collector",
                             ImmutableList.of("system.aa", "system.ab", "system.zz"));

    verifySearchMetricResult("/v3/metrics/search?target=metric&tag=namespace:myspace&tag=app:WordCount1" +
                               "&tag=flow:WordCounter&tag=dataset:*&tag=run:run1",
                             ImmutableList.of("system.aa", "system.ab", "system.reads",
                                              "system.writes", "system.zz", "user.reads", "user.writes"));

    // wrong namespace
    verifySearchMetricResult("/v3/metrics/search?target=metric&tag=namespace:yourspace&tag=app:WordCount1" +
                               "&tag=flow:WordCounter&tag=dataset:*&tag=run:run1&tag=flowlet:splitter",
                             ImmutableList.<String>of());


    // metrics in yourspace
    verifySearchMetricResult("/v3/metrics/search?target=metric&tag=namespace:yourspace&tag=app:WCount1" +
                               "&tag=flow:WCounter&tag=dataset:*&tag=run:run1&tag=flowlet:splitter",
                             ImmutableList.of("system.reads"));

    verifySearchMetricResult("/v3/metrics/search?target=metric&tag=namespace:yourspace",
                             ImmutableList.of("system.reads", "system.workerreads", "system.workerwrites"));

    // wrong namespace
    verifySearchMetricResult("/v3/metrics/search?target=metric&tag=namespace:myspace&tag=app:WCount1" +
                               "&tag=flow:WCounter&tag=dataset:*&tag=run:run1&tag=flowlet:splitter",
                             ImmutableList.<String>of());

    // verify "*"
    verifySearchMetricResult("/v3/metrics/search?target=metric&tag=namespace:myspace&tag=app:WordCount1" +
                               "&tag=flow:WordCounter&tag=dataset:*&tag=run:run1&tag=flowlet:*",
                             ImmutableList.of("system.aa", "system.ab", "system.reads",
                                              "system.writes", "system.zz", "user.reads", "user.writes"));
    verifySearchMetricResult("/v3/metrics/search?target=metric&tag=namespace:myspace&tag=app:WordCount1" +
                               "&tag=flow:*&tag=dataset:*&tag=run:run1",
                             ImmutableList.of("system.aa", "system.ab", "system.reads",
                                              "system.writes", "system.zz", "user.reads", "user.writes"));
  }
  
  /**
   * Used to test time range queries when requests are batched
   */
  class TimeSeriesSummary {
    Map<String, String> grouping;
    String metricName;
    long numPoints;
    long totalSum;

    public TimeSeriesSummary(Map<String, String> grouping, String metricName, long numPoints, long totalSum) {
      this.grouping = grouping;
      this.metricName = metricName;
      this.numPoints = numPoints;
      this.totalSum = totalSum;
    }

    public long getTotalSum() {
      return totalSum;
    }

    public long getNumPoints() {
      return numPoints;
    }

    public String getMetricName() {
      return metricName;
    }

    public Map<String, String> getGrouping() {
      return grouping;
    }
  }

  private  void batchTest(Map<String, QueryRequestFormat> jsonBatch,
                          ImmutableMap<String, QueryResult> expected) throws Exception {
    String url = "/v3/metrics/query";
    Map<String, MetricQueryResult> results =
      post(url, GSON.toJson(jsonBatch), new TypeToken<Map<String, MetricQueryResult>>() { }.getType());

    // check we have all the keys
    Assert.assertEquals(expected.keySet(), results.keySet());
    for (Map.Entry<String, MetricQueryResult> entry : results.entrySet()) {
      ImmutableList<TimeSeriesSummary> expectedTimeSeriesSummary = expected.get(entry.getKey()).getExpectedList();
      MetricQueryResult actualQueryResult = entry.getValue();
      compareQueryResults(expectedTimeSeriesSummary, actualQueryResult);
      Assert.assertEquals(expected.get(entry.getKey()).getExpectedResolution(), actualQueryResult.getResolution());
    }
  }

  private void compareQueryResults(ImmutableList<TimeSeriesSummary> expected, MetricQueryResult actual) {

    MetricQueryResult.TimeSeries[] actualTimeSeries = actual.getSeries();
    for (MetricQueryResult.TimeSeries actualTimeSery : actualTimeSeries) {
      TimeSeriesSummary expectedTimeSeriesSummary = findExpectedQueryResult(expected, actualTimeSery);
      Assert.assertNotNull(expectedTimeSeriesSummary);
      MetricQueryResult.TimeValue[] values = actualTimeSery.getData();
      long numPoints = 0;
      long totalSum = 0;
      for (MetricQueryResult.TimeValue tv : values) {
        numPoints++;
        totalSum += tv.getValue();
      }
      Assert.assertEquals(expectedTimeSeriesSummary.getNumPoints(), numPoints);
      Assert.assertEquals(expectedTimeSeriesSummary.getTotalSum(), totalSum);
    }
  }

  private TimeSeriesSummary findExpectedQueryResult(ImmutableList<TimeSeriesSummary> expected,
                                              MetricQueryResult.TimeSeries actualTimeSeries) {
    for (TimeSeriesSummary result : expected) {
      if (result.getGrouping().equals(actualTimeSeries.getGrouping()) &&
        result.getMetricName().equals(actualTimeSeries.getMetricName())) {
        return result;
      }
    }

    return null;
  }

  private void verifyAggregateQueryResult(String url, List<Long> expectedValue) throws Exception {
    MetricQueryResult queryResult = post(url, MetricQueryResult.class);
    for (int i = 0; i < queryResult.getSeries().length; i++) {
      Assert.assertEquals(expectedValue.get(i), (Long) queryResult.getSeries()[i].getData()[0].getValue());
    }
  }

  private void verifyRangeQueryResult(String url, List<Long> expectedPoints, List<Long> expectedSum) throws Exception {
    MetricQueryResult queryResult = post(url, MetricQueryResult.class);
    for (int i = 0; i < queryResult.getSeries().length; i++) {
      verifyTimeSeries(queryResult.getSeries()[i], expectedPoints.get(i), expectedSum.get(i));
    }
  }

  @Test
  public void testResultLimit() throws Exception {
    long start = 1;
    Map<String, String> sliceBy = getFlowletContext("resolutions", "WordCount1", "WordCounter", "run1", "splitter");

    // 1 second
    metricStore.add(new MetricValues(sliceBy, "reads", start, 1, MetricType.COUNTER));
    // 30 second
    metricStore.add(new MetricValues(sliceBy, "reads", start + 30, 1, MetricType.COUNTER));
    // 1 minute
    metricStore.add(new MetricValues(sliceBy, "reads", start + 60, 1, MetricType.COUNTER));
    // 10 minutes
    metricStore.add(new MetricValues(sliceBy, "reads", start + 600, 1, MetricType.COUNTER));
    // 1 hour
    metricStore.add(new MetricValues(sliceBy, "reads", start + 3600, 1, MetricType.COUNTER));

    // count is one record
    verifyRangeQueryResult(
      "/v3/metrics/query?" + getTags("resolutions", "WordCount1", "WordCounter", "splitter") +
        "&metric=system.reads&resolution=auto&count=1&start=" + start + "&end="
        + (start + 600), 1, 1);

    // count is greater than data points in time-range
    verifyRangeQueryResult(
      "/v3/metrics/query?" + getTags("resolutions", "WordCount1", "WordCounter", "splitter") +
        "&metric=system.reads&resolution=auto&count=6&start=" + start + "&end="
        + (start + 600), 4, 4);

    // count is less than data points in time-range
    verifyRangeQueryResult(
      "/v3/metrics/query?" + getTags("resolutions", "WordCount1", "WordCounter", "splitter") +
        "&metric=system.reads&resolution=auto&count=2&start=" + (start - 1) + "&end="
        + (start + 3600), 2, 3);
  }

  private void verifyAggregateQueryResult(String url, long expectedValue) throws Exception {
    // todo : can refactor this to test only the new tag name queries once we deprecate queryParam using context.
    MetricQueryResult queryResult = post(url, MetricQueryResult.class);
    Assert.assertEquals(expectedValue, queryResult.getSeries()[0].getData()[0].getValue());
  }

  private void verifyRangeQueryResult(String url, long nonZeroPointsCount, long expectedSum) throws Exception {
    MetricQueryResult queryResult = post(url, MetricQueryResult.class);
    verifyTimeSeries(queryResult.getSeries()[0], nonZeroPointsCount, expectedSum);
  }

  private void verifyTimeSeries(MetricQueryResult.TimeSeries timeSeries, long nonZeroPointsCount, long expectedSum) {
    MetricQueryResult.TimeValue[] data = timeSeries.getData();

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
    return post(url, null, type);
  }

  private <T> T post(String url, String body, Type type) throws Exception {
    HttpResponse response = doPost(url, body);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return GSON.fromJson(EntityUtils.toString(response.getEntity(), Charsets.UTF_8), type);
  }

  private void verifySearchMetricResult(String url, List<String> expectedValues) throws Exception {
    HttpResponse response = doPost(url, null);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String result = EntityUtils.toString(response.getEntity());
    List<String> reply = GSON.fromJson(result, new TypeToken<List<String>>() { }.getType());
    reply = removeMetricsSystemMetrics(reply);
    Assert.assertEquals(expectedValues.size(), reply.size());
    for (int i = 0; i < expectedValues.size(); i++) {
      Assert.assertEquals(expectedValues.get(i), reply.get(i));
    }
  }

  private List<String> removeMetricsSystemMetrics(List<String> reply) {
    List<String> result = Lists.newArrayList();
    for (String metric : reply) {
      if (!metric.startsWith("system.metrics") && !metric.startsWith("user.metrics")) {
        result.add(metric);
      }
    }
    return result;
  }

  private void verifySearchResultWithTags(String url, List<Map<String, String>> expectedValues) throws Exception {
    List<Map<String, String>> reply = getMetricsResults(url);
    Assert.assertTrue(reply.containsAll(expectedValues) && expectedValues.containsAll(reply));
  }

  // to support optional values in results.
  private List<Map<String, String>> getMetricsResults(String url) throws Exception {
    HttpResponse response = doPost(url, null);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String result = EntityUtils.toString(response.getEntity(), Charsets.UTF_8);
    return GSON.fromJson(result, new TypeToken<List<Map<String, String>>>() { }.getType());
  }

  private void verifySearchResultWithTags(String url, List<Map<String, String>> requiredValues,
                                          List<Map<String, String>> allValues)  throws Exception {
    List<Map<String, String>> reply = getMetricsResults(url);
    Assert.assertTrue(reply.containsAll(requiredValues) && allValues.containsAll(reply));
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
