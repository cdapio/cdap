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
  public void testFlow() {
    MetricsRequest request = MetricsRequestParser.parse(URI.create("/process/bytes/app1?count=60"));
    Assert.assertEquals("app1", request.getContextPrefix());
    Assert.assertEquals("bytes", request.getMetricPrefix());
    Assert.assertEquals(0, request.getStartTime());
    Assert.assertEquals(60, request.getCount());

    request = MetricsRequestParser.parse(URI.create("/process/bytes/app1/flows/flowId?count=60&start=1&end=2"));
    Assert.assertEquals("app1.f.flowId", request.getContextPrefix());
    Assert.assertEquals("bytes", request.getMetricPrefix());
    Assert.assertEquals(1, request.getStartTime());
    Assert.assertEquals(2, request.getEndTime());

    request = MetricsRequestParser.parse(URI.create("/process/bytes/app1/flows/flowId/flowletId?summary=true"));
    Assert.assertEquals("app1.f.flowId.flowletId", request.getContextPrefix());
    Assert.assertEquals(MetricsRequest.Type.SUMMARY, request.getType());

    request = MetricsRequestParser.parse(
                  URI.create("/process/events/app1/flows/flowId/flowletId/ins/queueId?aggregate=true"));
    Assert.assertEquals("app1.f.flowId.flowletId", request.getContextPrefix());
    Assert.assertEquals("events.ins.queueId", request.getMetricPrefix());
    Assert.assertEquals(MetricsRequest.Type.AGGREGATE, request.getType());
  }

  @Test
  public void testMapReduce() {
    MetricsRequest request = MetricsRequestParser.parse(
                                URI.create("/process/completion/app1/mapreduce/jobId/runId?summary=true"));
    Assert.assertEquals("app1.b.jobId", request.getContextPrefix());
    Assert.assertEquals("runId", request.getRunId());

    request = MetricsRequestParser.parse(
      URI.create("/process/completion/app1/mapreduce/jobId/runId/mappers?summary=true"));
    Assert.assertEquals("app1.b.jobId.m", request.getContextPrefix());
    Assert.assertEquals("runId", request.getRunId());

    request = MetricsRequestParser.parse(
      URI.create("/process/completion/app1/mapreduce/jobId/runId/mappers/mapperId?summary=true"));
    Assert.assertEquals("app1.b.jobId.m.mapperId", request.getContextPrefix());
    Assert.assertEquals("runId", request.getRunId());
    Assert.assertEquals(MetricsRequest.Type.SUMMARY, request.getType());
  }
}
