/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AppMetricsQueryTest extends BaseMetricsQueryTest {

  @Test
  public void testGetAppMetrics() throws InterruptedException, IOException {
    String flowId = "flow1";
    String flowletId = "flowlet1";
    String flowContext = "appId.f." + flowId + "." + flowletId;
    MetricsCollector flowletCollector = collectionService.getCollector(MetricsScope.USER,
                                                                       flowContext, "0");
    String[] flowletMetrics = {"metric1", "metric2"};
    for (int i = 0; i < flowletMetrics.length; i++) {
      flowletCollector.gauge(flowletMetrics[i], (i + 1) * 5);
    }

    String[] procedures = {"p1", "p2"};
    MetricsCollector[] procedureCollectors = new MetricsCollector[procedures.length];
    for (int i = 0; i < procedures.length; i++) {
      String procedureContext = "appId.p." + procedures[i];
      MetricsCollector collector = collectionService.getCollector(MetricsScope.USER, procedureContext, "0");
      collector.gauge("metric1", (i + 1) * 10);
    }

    TimeUnit.SECONDS.sleep(2);

    // Query for queue length
    InetSocketAddress endpoint = getMetricsQueryEndpoint();
    URLConnection urlConn = new URL(String.format("http://%s:%d/appmetrics/appId",
                                                  endpoint.getHostName(),
                                                  endpoint.getPort())).openConnection();
    urlConn.setDoOutput(true);
    urlConn.addRequestProperty("Content-type", "application/json");
    JsonWriter writer = new JsonWriter(new OutputStreamWriter(urlConn.getOutputStream(), Charsets.UTF_8));

    try {
      writer.beginObject();
      // write flowlet stuff
      writer.name("flowlets");
      writer.beginArray();
      for (String flowletMetric : flowletMetrics) {
        writer.beginObject();
        writer.name("flow").value(flowId);
        writer.name("flowlet").value(flowletId);
        writer.name("metricPrefix").value(flowletMetric);
        writer.endObject();
      }
      writer.endArray();
      // write procedure stuff
      writer.name("procedures");
      writer.beginArray();
      for (String procedure : procedures) {
        writer.beginObject();
        writer.name("procedure").value(procedure);
        writer.name("metricPrefix").value("metric1");
        writer.endObject();
      }
      writer.endArray();
      writer.endObject();
    } finally {
      writer.close();
    }
    Reader reader = new InputStreamReader(urlConn.getInputStream(), Charsets.UTF_8);
    try {
      /* Expected result looks like
       * {
       *   "flowlets": [
       *     { flowid.flowletid.metric: count}
       *     ...
       *   ],
       *   "procedures": [
       *     { procedureid.metric: count}
       *     ...
       *   ]
       * }
       */
      JsonObject json = new Gson().fromJson(reader, JsonObject.class);
      // check flowlet metrics
      JsonArray flowletMetricData = json.getAsJsonArray("flowlets");
      JsonObject metricData = flowletMetricData.get(0).getAsJsonObject();
      Assert.assertEquals(metricData.get("data").getAsLong(), 5L);
      Assert.assertEquals(metricData.get("metricPrefix").getAsString(), "metric1");
      metricData = flowletMetricData.get(1).getAsJsonObject();
      Assert.assertEquals(metricData.get("data").getAsLong(), 10L);
      Assert.assertEquals(metricData.get("metricPrefix").getAsString(), "metric2");
      // check procedure metrics
      JsonArray procedureMetricData = json.getAsJsonArray("procedures");
      metricData = procedureMetricData.get(0).getAsJsonObject();
      Assert.assertEquals(metricData.get("data").getAsLong(), 10L);
      Assert.assertEquals(metricData.get("procedure").getAsString(), "p1");
      metricData = procedureMetricData.get(1).getAsJsonObject();
      Assert.assertEquals(metricData.get("data").getAsLong(), 20L);
      Assert.assertEquals(metricData.get("procedure").getAsString(), "p2");
    } finally {
      reader.close();
    }
  }
}
