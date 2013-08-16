/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.engine.leveldb.LevelDBOVCTableHandle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.data.NoopTransactionOracle;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.metrics.guice.MetricsQueryRuntimeModule;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MetricsQueryTest extends BaseMetricsQueryTest {

  private static File dataDir;
  private static Injector injector;
  private static MetricsCollectionService collectionService;
  private static MetricsQueryService queryService;

  @Test
  public void testQueueLength() throws InterruptedException, IOException {
    QueueName queueName = QueueName.fromFlowlet("flowId", "flowlet1", "out");

    // Insert queue metrics
    MetricsCollector enqueueCollector = collectionService.getCollector(MetricsScope.REACTOR,
                                                                       "appId.f.flowId.flowlet1", "0");
    enqueueCollector.gauge("q.enqueue." + queueName.toString(), 10);

    // Insert ack metrics
    MetricsCollector ackCollector = collectionService.getCollector(MetricsScope.REACTOR,
                                                                   "appId.f.flowId.flowlet2", "0");
    ackCollector.gauge("q.ack." + queueName.toString(), 6);

    // Wait for collection to happen
    TimeUnit.SECONDS.sleep(2);

    // Query for queue length
    InetSocketAddress endpoint = getMetricsQueryEndpoint();
    URLConnection urlConn = new URL(String.format("http://%s:%d/metrics",
                                                  endpoint.getHostName(),
                                                  endpoint.getPort())).openConnection();
    urlConn.setDoOutput(true);
    urlConn.addRequestProperty("Content-type", "application/json");
    Writer writer = new OutputStreamWriter(urlConn.getOutputStream(), Charsets.UTF_8);
    try {
      new Gson().toJson(ImmutableList.of("/process/events/appId/flows/flowId/flowlet2/pending?aggregate=true"), writer);
    } finally {
      writer.close();
    }
    Reader reader = new InputStreamReader(urlConn.getInputStream(), Charsets.UTF_8);
    try {
      JsonElement json = new Gson().fromJson(reader, JsonElement.class);
      // Expected result looks like
      // [
      //   {
      //     "path":"/process/events/appId/flows/flowId/flowlet2/pending?aggregate=true",
      //     "result":{"data":4}
      //   }
      // ]
      JsonObject resultObj = json.getAsJsonArray().get(0).getAsJsonObject().get("result").getAsJsonObject();
      Assert.assertEquals(4, resultObj.getAsJsonPrimitive("data").getAsInt());
    } finally {
      reader.close();
    }
  }
}
