/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.metrics.data.Interpolators;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MetricsRequestParserTest {

  @Test
  public void testPathStrip() {
    String expected = "/reactor/apps/app1/flows/flow1/metric?aggregate=true";
    String path = Constants.Gateway.GATEWAY_VERSION + "/metrics" + expected;
    Assert.assertEquals(expected, MetricsRequestParser.stripVersionAndMetricsFromPath(path));
  }

  @Test
  public void testQueryArgs() throws MetricsPathException {
    MetricsRequest request = MetricsRequestParser.parse(URI.create("/reactor/apps/app1/reads?count=60"));
    Assert.assertEquals(MetricsRequest.Type.TIME_SERIES, request.getType());
    Assert.assertEquals(60, request.getCount());

    request = MetricsRequestParser.parse(URI.create("/reactor/apps/app1/reads?summary=true"));
    Assert.assertEquals(MetricsRequest.Type.SUMMARY, request.getType());

    request = MetricsRequestParser.parse(URI.create("/reactor/apps/app1/reads?aggregate=true"));
    Assert.assertEquals(MetricsRequest.Type.AGGREGATE, request.getType());

    request = MetricsRequestParser.parse(URI.create("/reactor/apps/app1/reads?count=60&start=1&end=61"));
    Assert.assertEquals(1, request.getStartTime());
    Assert.assertEquals(61, request.getEndTime());
    Assert.assertEquals(MetricsRequest.Type.TIME_SERIES, request.getType());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/reads?count=60&start=1&end=61"));
    Assert.assertEquals(1, request.getStartTime());
    Assert.assertEquals(61, request.getEndTime());
    Assert.assertEquals(MetricsRequest.Type.TIME_SERIES, request.getType());
    Assert.assertNull(request.getInterpolator());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/reads?start=1&end=61"));
    Assert.assertEquals(1, request.getStartTime());
    Assert.assertEquals(61, request.getEndTime());
    Assert.assertEquals(MetricsRequest.Type.TIME_SERIES, request.getType());
    Assert.assertNull(request.getInterpolator());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/reads?count=60&start=1&end=61&interpolate=step"));
    Assert.assertEquals(1, request.getStartTime());
    Assert.assertEquals(61, request.getEndTime());
    Assert.assertEquals(MetricsRequest.Type.TIME_SERIES, request.getType());
    Assert.assertTrue(request.getInterpolator() instanceof Interpolators.Step);

    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/reads?count=60&start=1&end=61&interpolate=linear"));
    Assert.assertEquals(1, request.getStartTime());
    Assert.assertEquals(61, request.getEndTime());
    Assert.assertEquals(MetricsRequest.Type.TIME_SERIES, request.getType());
    Assert.assertTrue(request.getInterpolator() instanceof Interpolators.Linear);
  }

  @Test
  public void testRelativeTimeArgs() throws MetricsPathException  {
    long now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/reads?count=61&end=now-5s"));
    assertTimestamp(now - 5, request.getEndTime());
    assertTimestamp(now - 65, request.getStartTime());

    now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/reads?count=61&start=now-65s"));
    assertTimestamp(now - 5, request.getEndTime());
    assertTimestamp(now - 65, request.getStartTime());

    now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/reads?count=61&start=now-1m"));
    assertTimestamp(now, request.getEndTime());
    assertTimestamp(now - 60, request.getStartTime());

    now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/reads?count=61&start=now-1h"));
    assertTimestamp(now - 3600 + 60, request.getEndTime());
    assertTimestamp(now - 3600, request.getStartTime());

    now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/reads?count=61&start=now-1d"));
    assertTimestamp(now - 86400 + 60, request.getEndTime());
    assertTimestamp(now - 86400, request.getStartTime());

    now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/reads?count=61&start=now-1m&end=now"));
    assertTimestamp(now, request.getEndTime());
    assertTimestamp(now - 60, request.getStartTime());

    now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/reads?count=61&start=now-2m%2B20s"));
    assertTimestamp(now - 40, request.getEndTime());
    assertTimestamp(now - 100, request.getStartTime());
  }

  // assuming you got the actual timestamp after the expected, check that they are equal,
  // or that the actual is 1 second before the expected in case we were on a second boundary.
  private void assertTimestamp(long expected, long actual) {
    Assert.assertTrue(actual + " not within 1 second of " + expected, expected == actual || (actual - 1) == expected);
  }

  @Test
  public void testScope() throws MetricsPathException {
    MetricsRequest request = MetricsRequestParser.parse(URI.create("/reactor/apps/app1/reads?summary=true"));
    Assert.assertEquals(MetricsScope.REACTOR, request.getScope());

    request = MetricsRequestParser.parse(URI.create("/user/apps/app1/reads?summary=true"));
    Assert.assertEquals(MetricsScope.USER, request.getScope());
  }

  @Test
  public void testOverview() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(URI.create("/reactor/reads?aggregate=true"));
    Assert.assertNull(request.getContextPrefix());
    Assert.assertEquals("reads", request.getMetricPrefix());
  }

  @Test
  public void testApps() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(URI.create("/reactor/apps/app1/reads?aggregate=true"));
    Assert.assertEquals("app1", request.getContextPrefix());
    Assert.assertEquals("reads", request.getMetricPrefix());
  }

  @Test
  public void testFlow() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/flows/flow1/flowlets/flowlet1/process.bytes?count=60&start=1&end=61"));
    Assert.assertEquals("app1.f.flow1.flowlet1", request.getContextPrefix());
    Assert.assertEquals("process.bytes", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/flows/flow1/some.metric?summary=true"));
    Assert.assertEquals("app1.f.flow1", request.getContextPrefix());
    Assert.assertEquals("some.metric", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/flows/loads?aggregate=true"));
    Assert.assertEquals("app1.f", request.getContextPrefix());
    Assert.assertEquals("loads", request.getMetricPrefix());
  }

  @Test
  public void testQueues() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/flows/flow1/flowlets/flowlet1/queues/queue1/process.bytes.in?aggregate=true"));
    Assert.assertEquals("app1.f.flow1.flowlet1", request.getContextPrefix());
    Assert.assertEquals("process.bytes.in", request.getMetricPrefix());
    Assert.assertEquals("queue1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/flows/flow1/flowlets/flowlet1/queues/queue1/process.bytes.out?aggregate=true"));
    Assert.assertEquals("app1.f.flow1.flowlet1", request.getContextPrefix());
    Assert.assertEquals("process.bytes.out", request.getMetricPrefix());
    Assert.assertEquals("queue1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/flows/flow1/flowlets/flowlet1/queues/queue1/process.events.in?aggregate=true"));
    Assert.assertEquals("app1.f.flow1.flowlet1", request.getContextPrefix());
    Assert.assertEquals("process.events.in", request.getMetricPrefix());
    Assert.assertEquals("queue1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/flows/flow1/flowlets/flowlet1/queues/queue1/process.events.out?aggregate=true"));
    Assert.assertEquals("app1.f.flow1.flowlet1", request.getContextPrefix());
    Assert.assertEquals("process.events.out", request.getMetricPrefix());
    Assert.assertEquals("queue1", request.getTagPrefix());
  }

  @Test
  public void testMapReduce() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/mapreduce/mapred1/mappers/reads?summary=true"));
    Assert.assertEquals("app1.b.mapred1.m", request.getContextPrefix());
    Assert.assertEquals("reads", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/mapreduce/mapred1/reducers/reads?summary=true"));
    Assert.assertEquals("app1.b.mapred1.r", request.getContextPrefix());
    Assert.assertEquals("reads", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/mapreduce/mapred1/reads?summary=true"));
    Assert.assertEquals("app1.b.mapred1", request.getContextPrefix());
    Assert.assertEquals("reads", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/mapreduce/reads?summary=true"));
    Assert.assertEquals("app1.b", request.getContextPrefix());
    Assert.assertEquals("reads", request.getMetricPrefix());
  }

  @Test
  public void testProcedure() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/procedures/proc1/reads?summary=true"));
    Assert.assertEquals("app1.p.proc1", request.getContextPrefix());
    Assert.assertEquals("reads", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/procedures/reads?summary=true"));
    Assert.assertEquals("app1.p", request.getContextPrefix());
    Assert.assertEquals("reads", request.getMetricPrefix());
  }

  @Test
  public void testUserServices() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/services/serve1/reads?summary=true"));
    Assert.assertEquals("app1.s.serve1", request.getContextPrefix());
    Assert.assertEquals("reads", request.getMetricPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/services/serve1/runnables/run1/reads?summary=true"));
    Assert.assertEquals("app1.s.serve1.run1", request.getContextPrefix());
    Assert.assertEquals("reads", request.getMetricPrefix());
  }


  @Test(expected = MetricsPathException.class)
  public void testInvalidUserServices() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/service/serve1/reads?summary=true"));
  }

  @Test(expected = MetricsPathException.class)
  public void testInvalidUserServicesTooManyPath() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/apps/app1/services/serve1/runnables/run1/random/reads?summary=true"));
  }

  @Test
  public void testDataset() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/datasets/dataset1/apps/app1/flows/flow1/flowlets/flowlet1/store.reads?summary=true"));
    Assert.assertEquals("app1.f.flow1.flowlet1", request.getContextPrefix());
    Assert.assertEquals("store.reads", request.getMetricPrefix());
    Assert.assertEquals("dataset1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/datasets/dataset1/apps/app1/flows/flow1/store.reads?summary=true"));
    Assert.assertEquals("app1.f.flow1", request.getContextPrefix());
    Assert.assertEquals("store.reads", request.getMetricPrefix());
    Assert.assertEquals("dataset1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/datasets/dataset1/apps/app1/flows/store.reads?summary=true"));
    Assert.assertEquals("app1.f", request.getContextPrefix());
    Assert.assertEquals("store.reads", request.getMetricPrefix());
    Assert.assertEquals("dataset1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/datasets/dataset1/apps/app1/store.reads?summary=true"));
    Assert.assertEquals("app1", request.getContextPrefix());
    Assert.assertEquals("store.reads", request.getMetricPrefix());
    Assert.assertEquals("dataset1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/reactor/datasets/dataset1/store.reads?summary=true"));
    Assert.assertNull(request.getContextPrefix());
    Assert.assertEquals("store.reads", request.getMetricPrefix());
    Assert.assertEquals("dataset1", request.getTagPrefix());
  }

  @Test
  public void testStream() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/streams/stream1/collect.events?summary=true"));
    Assert.assertNull(request.getContextPrefix());
    Assert.assertEquals("collect.events", request.getMetricPrefix());
    Assert.assertEquals("stream1", request.getTagPrefix());
  }


  @Test
  public void testService() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/services/appfabric/request.received?aggregate=true"));
    Assert.assertEquals("appfabric", request.getContextPrefix());
    Assert.assertEquals("request.received", request.getMetricPrefix());
  }


  @Test
  public void testHandler() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/services/appfabric/handlers/AppFabricHttpHandler/response.server-error?aggregate=true"));
    Assert.assertEquals("appfabric.AppFabricHttpHandler", request.getContextPrefix());
    Assert.assertEquals("response.server-error", request.getMetricPrefix());
  }

  @Test
  public void testMethod() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/services/metrics/handlers/MetricsQueryHandler/methods/handleComponent/" +
                   "response.successful?aggregate=true"));
    Assert.assertEquals("metrics.MetricsQueryHandler.handleComponent", request.getContextPrefix());
    Assert.assertEquals("response.successful", request.getMetricPrefix());
  }

  @Test(expected = MetricsPathException.class)
  public void testInvalidRequest() throws MetricsPathException {
    //handler instead of handlers
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/services/metrics/handler/MetricsQueryHandler/" +
                   "response.successful?aggregate=true"));
  }

  @Test
  public void testCluster() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/cluster/resources.total.storage?count=1&start=12345678&interpolate=step"));
    Assert.assertEquals("-.cluster", request.getContextPrefix());
    Assert.assertEquals("resources.total.storage", request.getMetricPrefix());
  }


  @Test
  public void testTransactions() throws MetricsPathException  {
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/reactor/transactions/invalid?count=1&start=12345678&interpolate=step"));
    Assert.assertEquals("transactions", request.getContextPrefix());
    Assert.assertEquals("invalid", request.getMetricPrefix());
  }

  @Test
  public void testMetricURIDecoding() throws UnsupportedEncodingException, MetricsPathException {
    String weirdMetric = "/weird?me+tr ic#$name////";
    // encoded version or weirdMetric
    String encodedWeirdMetric = "%2Fweird%3Fme%2Btr%20ic%23%24name%2F%2F%2F%2F";
    MetricsRequest request = MetricsRequestParser.parse(
      URI.create("/user/apps/app1/flows/" + encodedWeirdMetric + "?aggregate=true"));
    Assert.assertEquals("app1.f", request.getContextPrefix());
    Assert.assertEquals(weirdMetric, request.getMetricPrefix());
    Assert.assertEquals(MetricsScope.USER, request.getScope());

    request = MetricsRequestParser.parse(
      URI.create("/user/apps/app1/" + encodedWeirdMetric + "?aggregate=true"));
    Assert.assertEquals("app1", request.getContextPrefix());
    Assert.assertEquals(weirdMetric, request.getMetricPrefix());
    Assert.assertEquals(MetricsScope.USER, request.getScope());
  }


  @Test(expected = IllegalArgumentException.class)
  public void testUserMetricBadURIThrowsException() throws MetricsPathException {
    String badEncoding = "/%2";
    MetricsRequestParser.parse(URI.create("/user/apps/app1/flows" + badEncoding + "?aggregate=true"));
  }

  @Test
  public void testBadPathsThrowExceptions() {
    int numBad = 0;
    String[] validPaths = {
      "/reactor/metric?aggregate=true",
      "/reactor/apps/appX/metric?aggregate=true",
      "/reactor/apps/appX/flows/metric?aggregate=true",
      "/reactor/apps/appX/flows/flowY/metric?aggregate=true",
      "/reactor/apps/appX/flows/flowY/flowlets/flowletZ/metric?aggregate=true",
      "/reactor/apps/appX/procedures/metric?aggregate=true",
      "/reactor/apps/appX/procedures/procedureY/metric?aggregate=true",
      "/reactor/apps/appX/mapreduce/metric?aggregate=true",
      "/reactor/apps/appX/mapreduce/mapreduceY/metric?aggregate=true",
      "/reactor/apps/appX/mapreduce/mapreduceY/mappers/metric?aggregate=true",
      "/reactor/apps/appX/mapreduce/mapreduceY/reducers/metric?aggregate=true",
      "/reactor/datasets/datasetA/metric?aggregate=true",
      "/reactor/datasets/datasetA/apps/appX/metric?aggregate=true",
      "/reactor/datasets/datasetA/apps/appX/flows/flowY/metric?aggregate=true",
      "/reactor/datasets/datasetA/apps/appX/flows/flowY/flowlets/flowletZ/metric?aggregate=true",
      "/reactor/streams/streamA/metric?aggregate=true"
    };
    // check that miss-spelled paths and the like throw an exception.
    String[] invalidPaths = {
      "/reacto/metric?aggregate=true",
      "/reactor/app/appX/metric?aggregate=true",
      "/reactor/apps/appX/flow/metric?aggregate=true",
      "/reactor/apps/appX/flows/flowY/flowlet/flowletZ/metric?aggregate=true",
      "/reactor/apps/appX/procedure/metric?aggregate=true",
      "/reactor/apps/appX/procedure/procedureY/metric?aggregate=true",
      "/reactor/apps/appX/mapreduces/metric?aggregate=true",
      "/reactor/apps/appX/mapreduces/mapreduceY/metric?aggregate=true",
      "/reactor/apps/appX/mapreduce/mapreduceY/mapper/metric?aggregate=true",
      "/reactor/apps/appX/mapreduce/mapreduceY/reducer/metric?aggregate=true",
      "/reactor/dataset/datasetA/metric?aggregate=true",
      "/reactor/datasets/datasetA/app/appX/metric?aggregate=true",
      "/reactor/datasets/datasetA/apps/appX/flow/flowY/metric?aggregate=true",
      "/reactor/datasets/datasetA/apps/appX/flows/flowY/flowlet/flowletZ/metric?aggregate=true",
      "/reactor/stream/streamA/metric?aggregate=true"
    };
    for (String path : validPaths) {
      try {
        MetricsRequest request = MetricsRequestParser.parse(URI.create(path));
      } catch (MetricsPathException e) {
        numBad++;
      }
    }
    Assert.assertEquals(0, numBad);
    for (String path : invalidPaths) {
      try {
        MetricsRequest request = MetricsRequestParser.parse(URI.create(path));
      } catch (MetricsPathException e) {
        numBad++;
      }
    }
    Assert.assertEquals(invalidPaths.length, numBad);
  }
}
