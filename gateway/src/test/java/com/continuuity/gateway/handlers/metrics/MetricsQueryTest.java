/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.gateway.handlers.metrics;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.common.queue.QueueName;
import com.continuuity.gateway.MetricsServiceTestsSuite;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MetricsQueryTest extends BaseMetricsQueryTest {

  @Test
  public void testQueueLength() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("WordCount", "WordCounter", "counter", "queue");

    // Insert queue metrics
    MetricsCollector enqueueCollector = collectionService.getCollector(MetricsScope.REACTOR,
                                                                       "WordCount.f.WordCounter.counter", "0");
    enqueueCollector.gauge("process.events.out", 10, queueName.getSimpleName());

    // Insert ack metrics
    MetricsCollector ackCollector = collectionService.getCollector(MetricsScope.REACTOR,
                                                                   "WordCount.f.WordCounter.unique", "0");
    ackCollector.gauge("process.events.processed", 6, "input." + queueName.toString());
    ackCollector.gauge("process.events.processed", 2, "input.stream:///streamX");
    ackCollector.gauge("process.events.processed", 1, "input.stream://developer/streamX");

    // Insert stream metrics
    MetricsCollector streamCollector = collectionService.getCollector(MetricsScope.REACTOR,
                                                                      Constants.Gateway.METRICS_CONTEXT, "0");
    streamCollector.gauge("collect.events", 5, "streamX");

    // Wait for collection to happen
    TimeUnit.SECONDS.sleep(2);

    // Query for queue length
    HttpPost post = MetricsServiceTestsSuite.getPost("/v2/metrics");
    post.setHeader("Content-type", "application/json");
    post.setEntity(new StringEntity(
      "[\"/reactor/apps/WordCount/flows/WordCounter/flowlets/unique/process.events.pending?aggregate=true\"]"));
    HttpResponse response = MetricsServiceTestsSuite.doPost(post);
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    try {
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
      JsonElement json = new Gson().fromJson(reader, JsonElement.class);
      // Expected result looks like
      // [
      //   {
      //     "path":"/process/events/appId/flows/flowId/flowlet2/pending?aggregate=true",
      //     "result":{"data":6}
      //   }
      // ]
      JsonObject resultObj = json.getAsJsonArray().get(0).getAsJsonObject().get("result").getAsJsonObject();
      Assert.assertEquals(6, resultObj.getAsJsonPrimitive("data").getAsInt());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testGetMetric() throws Exception {
    // Insert some metric
    MetricsCollector collector = collectionService.getCollector(MetricsScope.REACTOR,
                                                                "WordCount.f.WordCounter.counter", "0");
    collector.gauge("reads", 10, "wordStats");
    collector.gauge("collect.events", 10, "wordStream");
    collector = collectionService.getCollector(MetricsScope.REACTOR, "-.cluster", "0");
    collector.gauge("resources.total.storage", 10);

    // Wait for collection to happen
    TimeUnit.SECONDS.sleep(2);

    for (String resource : validResources) {
      HttpResponse response = MetricsServiceTestsSuite.doGet("/v2/metrics" + resource);
      Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
      try {
        Assert.assertEquals("GET " + resource + " did not return 200 status.",
                            HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        JsonObject json = new Gson().fromJson(reader, JsonObject.class);
        // Expected result looks like
        // {
        //   "result":{"data":10}
        // }
        Assert.assertEquals("GET " + resource + " returned unexpected results.", 10, json.get("data").getAsInt());
      } finally {
        reader.close();
      }
    }
  }

  @Test
  public void testInvalidPathReturns404() throws Exception {
    for (String resource : invalidResources) {
      // test GET request fails with 404
      HttpResponse response = MetricsServiceTestsSuite.doGet("/v2/metrics" + resource);
      Assert.assertEquals("GET " + resource + " did not return 404 as expected.",
                          HttpStatus.SC_NOT_FOUND, response.getStatusLine().getStatusCode());
      // test POST also fails, but with 400
      HttpPost post = MetricsServiceTestsSuite.getPost("/v2/metrics");
      post.setHeader("Content-type", "application/json");
      post.setEntity(new StringEntity("[\"" + resource + "\"]"));
      response = MetricsServiceTestsSuite.doPost(post);
      Assert.assertEquals("POST for " + resource + " did not return 400 as expected.",
                          HttpStatus.SC_BAD_REQUEST, response.getStatusLine().getStatusCode());
    }
  }
}
