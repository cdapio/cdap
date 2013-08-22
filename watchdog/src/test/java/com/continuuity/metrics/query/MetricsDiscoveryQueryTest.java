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
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
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
public class MetricsDiscoveryQueryTest extends BaseMetricsQueryTest {

  @Test
  public void testDiscoverAllMetrics() throws InterruptedException, IOException {
    MetricsCollector collector = collectionService.getCollector(MetricsScope.USER, "app1.f.flow1.flowlet1", "0");
    collector.gauge("reads", 1);
    collector.gauge("writes", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app2.f.flow1.flowlet1", "0");
    collector.gauge("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app2.f.flow1.flowlet2", "0");
    collector.gauge("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app2.f.flow2.flowlet1", "0");
    collector.gauge("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app2.p.procedure1", "0");
    collector.gauge("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app2.p.procedure2", "0");
    collector.gauge("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app2.b.mapred1.mapper", "0");
    collector.gauge("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app2.b.mapred2.mapper", "0");
    collector.gauge("reads", 1);
    collector = collectionService.getCollector(MetricsScope.USER, "app2.b.mapred2.reducer", "0");
    collector.gauge("reads", 1);


    Reader fileReader = new InputStreamReader(new FileInputStream("watchdog/src/test/data/MetricDiscoveryOutput.json"));
    JsonParser parser = new JsonParser();
    JsonArray expected = parser.parse(fileReader).getAsJsonArray();

    TimeUnit.SECONDS.sleep(2);
    // Query for queue length
    InetSocketAddress endpoint = getMetricsQueryEndpoint();
    URLConnection urlConn = new URL(String.format("http://%s:%d/metrics/available",
                                                  endpoint.getHostName(),
                                                  endpoint.getPort())).openConnection();
    urlConn.setDoOutput(true);
    Reader reader = new InputStreamReader(urlConn.getInputStream(), Charsets.UTF_8);
    try {
      JsonArray json = new Gson().fromJson(reader, JsonArray.class);
      Assert.assertEquals(expected, json);
      String dumb = "dumb";
      dumb += ".";
    } finally {
      reader.close();
    }
  }
}
