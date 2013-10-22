/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MetricsDiscoveryQueryTest extends BaseMetricsQueryTest {

  @Test
  public void testDiscoverMetrics() throws InterruptedException, IOException {
    setupMetrics();

    JsonArray expected = new JsonArray();

    JsonArray readContexts =
      children(
        node("app", "app20", children(
          node("flow", "flow1", children(
            node("flowlet", "flowlet1"))),
          node("flow", "flow2", children(
            node("flowlet", "flowlet1"),
            node("flowlet", "flowlet2"))),
          node("mapreduce", "mapred1", children(
            node("mapreduceTask", "mappers"),
            node("mapreduceTask", "reducers"))),
          node("mapreduce", "mapred2", children(
            node("mapreduceTask", "mappers"))),
          node("procedure", "procedure1"),
          node("procedure", "procedure2"))));

    JsonObject reads = new JsonObject();
    reads.addProperty("metric", "reads");
    reads.add("contexts", readContexts);
    expected.add(reads);

    // Query for queue length
    InetSocketAddress endpoint = getMetricsQueryEndpoint();
    URLConnection urlConn = new URL(String.format("http://%s:%d%s/metrics/available/apps/app20",
                                                  endpoint.getHostName(),
                                                  endpoint.getPort(),
                                                  Constants.Gateway.GATEWAY_VERSION)).openConnection();
    urlConn.setDoOutput(true);
    Reader reader = new InputStreamReader(urlConn.getInputStream(), Charsets.UTF_8);
    try {
      JsonArray json = new Gson().fromJson(reader, JsonArray.class);
      Assert.assertEquals(expected, json);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testFilters() throws InterruptedException, IOException {
    setupMetrics();

    JsonArray expected = new JsonArray();
    JsonArray contexts =
      children(
        node("app", "app10", children(
          node("flow", "flow1", children(
            node("flowlet", "flowlet1"))))));
    JsonObject expectedReads = new JsonObject();
    expectedReads.addProperty("metric", "reads");
    expectedReads.add("contexts", contexts);
    expected.add(expectedReads);
    expected.add(expectedWrites());

    // Query for queue length
    InetSocketAddress endpoint = getMetricsQueryEndpoint();
    URLConnection urlConn =
      new URL(String.format("http://%s:%d%s/metrics/available/apps/app10/flows/flow1/flowlets/flowlet1",
                            endpoint.getHostName(),
                            endpoint.getPort(),
                            Constants.Gateway.GATEWAY_VERSION)).openConnection();
    urlConn.setDoOutput(true);
    Reader reader = new InputStreamReader(urlConn.getInputStream(), Charsets.UTF_8);
    try {
      JsonArray json = new Gson().fromJson(reader, JsonArray.class);
      Assert.assertEquals(expected, json);
    } finally {
      reader.close();
    }
  }

  private void setupMetrics() throws InterruptedException {
    MetricsCollector collector = collectionService.getCollector(MetricsScope.USER, "app10.f.flow1.flowlet1", "0");
    collector.gauge("reads", 1);
    collector.gauge("writes", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app20.f.flow1.flowlet1", "0");
    collector.gauge("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app20.f.flow2.flowlet1", "0");
    collector.gauge("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app20.f.flow2.flowlet2", "0");
    collector.gauge("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app20.p.procedure1", "0");
    collector.gauge("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app20.p.procedure2", "0");
    collector.gauge("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app20.b.mapred1.m", "0");
    collector.gauge("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app20.b.mapred1.r", "0");
    collector.gauge("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app20.b.mapred2.m", "0");
    collector.gauge("reads", 1);

    // need a better way to do this
    TimeUnit.SECONDS.sleep(2);
  }

  private JsonObject expectedWrites() {
    JsonArray writeContexts =
      children(
        node("app", "app10", children(
          node("flow", "flow1", children(
            node("flowlet", "flowlet1"))))));

    JsonObject writes = new JsonObject();
    writes.addProperty("metric", "writes");
    writes.add("contexts", writeContexts);
    return writes;
  }

  private JsonObject node(String type, String id) {
    JsonObject out = new JsonObject();
    out.addProperty("type", type);
    out.addProperty("id", id);
    return out;
  }

  private JsonObject node(String type, String id, JsonArray children) {
    JsonObject out = new JsonObject();
    out.addProperty("type", type);
    out.addProperty("id", id);
    out.add("children", children);
    return out;
  }

  private JsonArray children(JsonObject... objects) {
    JsonArray out = new JsonArray();
    for (JsonObject obj : objects) {
      out.add(obj);
    }
    return out;
  }
}
