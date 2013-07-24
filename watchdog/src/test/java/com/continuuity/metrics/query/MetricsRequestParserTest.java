/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import junit.framework.Assert;
import org.junit.Test;

import java.net.URI;

/**
 *
 */
public class MetricsRequestParserTest {

  @Test
  public void testOverview() {
    MetricsRequest request = MetricsRequestParser.parse(URI.create("/process/busyness?count=60"));
    Assert.assertNull(request.getContextPrefix());
    Assert.assertEquals("process.busyness", request.getMetricPrefix());
  }

  @Test
  public void testFlow() {
    MetricsRequest request = MetricsRequestParser.parse(URI.create("/process/bytes/app1?count=60"));
    Assert.assertEquals("app1", request.getContextPrefix());
    Assert.assertEquals("process.bytes", request.getMetricPrefix());
    Assert.assertEquals(60, request.getCount());

    request = MetricsRequestParser.parse(URI.create("/process/bytes/app1/flows/flowId?count=60&start=1&end=61"));
    Assert.assertEquals("app1.f.flowId", request.getContextPrefix());
    Assert.assertEquals("process.bytes", request.getMetricPrefix());
    Assert.assertEquals(1, request.getStartTime());
    Assert.assertEquals(61, request.getEndTime());

    request = MetricsRequestParser.parse(URI.create("/process/bytes/app1/flows/flowId/flowletId?summary=true"));
    Assert.assertEquals("app1.f.flowId.flowletId", request.getContextPrefix());
    Assert.assertEquals(MetricsRequest.Type.SUMMARY, request.getType());

    request = MetricsRequestParser.parse(
                  URI.create("/process/events/app1/flows/flowId/flowletId/ins/queueId?aggregate=true"));
    Assert.assertEquals("app1.f.flowId.flowletId", request.getContextPrefix());
    Assert.assertEquals("process.events.ins.queueId", request.getMetricPrefix());
    Assert.assertEquals(MetricsRequest.Type.AGGREGATE, request.getType());
  }

  @Test
  public void testMapReduce() {
    MetricsRequest request = MetricsRequestParser.parse(
                                URI.create("/process/completion/app1/mapreduce/jobId?summary=true"));
    Assert.assertEquals("app1.b.jobId", request.getContextPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/process/completion/app1/mapreduce/jobId/mappers?summary=true"));
    Assert.assertEquals("app1.b.jobId.m", request.getContextPrefix());

    request = MetricsRequestParser.parse(
      URI.create("/process/completion/app1/mapreduce/jobId/mappers/mapperId?summary=true"));
    Assert.assertEquals("app1.b.jobId.m.mapperId", request.getContextPrefix());
    Assert.assertEquals(MetricsRequest.Type.SUMMARY, request.getType());
  }

  @Test
  public void testProcessEvents() {
    MetricsRequest request = MetricsRequestParser.parse(URI.create("/process/events/app1/flows/flowId?summary=true"));
    Assert.assertEquals("app1.f.flowId", request.getContextPrefix());
    Assert.assertEquals("process.events.processed", request.getMetricPrefix());

    request = MetricsRequestParser.parse(URI.create("/process/events/app1/flows/flowId/flowletId?summary=true"));
    Assert.assertEquals("app1.f.flowId.flowletId", request.getContextPrefix());
    Assert.assertEquals("process.events.processed", request.getMetricPrefix());

    request = MetricsRequestParser.parse(URI.create("/process/events/app1/flows/flowId/flowletId/ins/q?summary=true"));
    Assert.assertEquals("app1.f.flowId.flowletId", request.getContextPrefix());
    Assert.assertEquals("process.events.ins.q", request.getMetricPrefix());
  }

  @Test
  public void testStoreApp() {
    MetricsRequest request = MetricsRequestParser.parse(URI.create("/store/bytes/apps/app1?aggregate=true"));
    Assert.assertEquals("app1", request.getContextPrefix());
    Assert.assertEquals("store.bytes", request.getMetricPrefix());

    request = MetricsRequestParser.parse(URI.create("/store/bytes/apps/app1/flows/flowId?aggregate=true"));
    Assert.assertEquals("app1.f.flowId", request.getContextPrefix());

    request = MetricsRequestParser.parse(
                        URI.create("/store/bytes/apps/app1/flows/flowId/flowletId/datasets/dataset1?aggregate=true"));
    Assert.assertEquals("app1.f.flowId.flowletId", request.getContextPrefix());
    Assert.assertEquals("store.bytes", request.getMetricPrefix());
    Assert.assertEquals("dataset1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
                        URI.create("/store/reads/apps/app1/mapreduce/jobId/datasets/dataset1?aggregate=true"));
    Assert.assertEquals("app1.b.jobId", request.getContextPrefix());
    Assert.assertEquals("store.reads", request.getMetricPrefix());
    Assert.assertEquals("dataset1", request.getTagPrefix());
  }

  @Test
  public void testStoreDataset() {
    MetricsRequest request = MetricsRequestParser.parse(
                                      URI.create("/store/reads/datasets/dataset1/app1/flows/flowId?aggregate=true"));
    Assert.assertEquals("app1.f.flowId", request.getContextPrefix());
    Assert.assertEquals("store.reads", request.getMetricPrefix());
    Assert.assertEquals("dataset1", request.getTagPrefix());

    request = MetricsRequestParser.parse(
                                    URI.create("/store/reads/datasets/dataset2/app1/mapreduce/jobId?aggregate=true"));
    Assert.assertEquals("app1.b.jobId", request.getContextPrefix());
    Assert.assertEquals("store.reads", request.getMetricPrefix());
    Assert.assertEquals("dataset2", request.getTagPrefix());

    request = MetricsRequestParser.parse(URI.create("/store/bytes/datasets/dataset2?aggregate=true"));
    Assert.assertNull(request.getContextPrefix());
    Assert.assertEquals("dataset2", request.getTagPrefix());
  }

  @Test
  public void testCollect() {
    MetricsRequest request = MetricsRequestParser.parse(
                                URI.create("/collect/events/apps/app1?aggregate=true"));
    Assert.assertEquals("app1", request.getContextPrefix());
    Assert.assertEquals("collect.events", request.getMetricPrefix());

    request = MetricsRequestParser.parse(URI.create("/collect/events/streams/stream1?aggregate=true"));
    Assert.assertNull(request.getContextPrefix());
    Assert.assertEquals("stream1", request.getTagPrefix());

    request = MetricsRequestParser.parse(URI.create("/collect/events/streams/stream1/app1/flows/flow1?aggregate=true"));
    Assert.assertEquals("app1.f.flow1", request.getContextPrefix());
  }
}
