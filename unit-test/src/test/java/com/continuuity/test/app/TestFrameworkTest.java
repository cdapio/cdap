package com.continuuity.test.app;

import com.continuuity.api.data.OperationException;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.MapReduceManager;
import com.continuuity.test.ProcedureClient;
import com.continuuity.test.ProcedureManager;
import com.continuuity.test.ReactorTestBase;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class TestFrameworkTest extends ReactorTestBase {

  @Test(timeout = 240000)
  public void testMultiInput() throws InterruptedException, IOException, TimeoutException {
    ApplicationManager applicationManager = deployApplication(JoinMultiStreamApp.class);
    try {
      applicationManager.startFlow("JoinMultiFlow");

      StreamWriter s1 = applicationManager.getStreamWriter("s1");
      StreamWriter s2 = applicationManager.getStreamWriter("s2");
      StreamWriter s3 = applicationManager.getStreamWriter("s3");

      s1.send("testing 1");
      s2.send("testing 2");
      s3.send("testing 3");

      RuntimeMetrics terminalMetrics = RuntimeStats.getFlowletMetrics("JoinMulti", "JoinMultiFlow", "Terminal");

      terminalMetrics.waitForProcessed(3, 5, TimeUnit.SECONDS);

      TimeUnit.SECONDS.sleep(1);

      ProcedureManager queryManager = applicationManager.startProcedure("Query");
      Gson gson = new Gson();

      ProcedureClient client = queryManager.getClient();
      Assert.assertEquals("testing 1",
                          gson.fromJson(client.query("get", ImmutableMap.of("key", "input1")), String.class));
      Assert.assertEquals("testing 2",
                          gson.fromJson(client.query("get", ImmutableMap.of("key", "input2")), String.class));
      Assert.assertEquals("testing 3",
                          gson.fromJson(client.query("get", ImmutableMap.of("key", "input3")), String.class));

    } finally {
      applicationManager.stopAll();
      clear();
    }
  }

  @Test(timeout = 360000)
  public void testApp() throws InterruptedException, IOException, TimeoutException, OperationException {
    ApplicationManager applicationManager = deployApplication(WordCountApp2.class);

    try {
      applicationManager.startFlow("WordCountFlow");

      // Send some inputs to streams
      StreamWriter streamWriter = applicationManager.getStreamWriter("text");
      for (int i = 0; i < 100; i++) {
        streamWriter.send(ImmutableMap.of("title", "title " + i), "testing message " + i);
      }

      // Check the flowlet metrics
      RuntimeMetrics flowletMetrics = RuntimeStats.getFlowletMetrics("WordCountApp",
                                                                     "WordCountFlow",
                                                                     "CountByField");
      flowletMetrics.waitForProcessed(500, 5, TimeUnit.SECONDS);
      Assert.assertEquals(0L, flowletMetrics.getException());

      // Query the result
      ProcedureManager procedureManager = applicationManager.startProcedure("WordFrequency");
      ProcedureClient procedureClient = procedureManager.getClient();

      // Verify the query result
      Type resultType = new TypeToken<Map<String, Long>>(){}.getType();
      Gson gson = new Gson();
      Map<String, Long> result = gson.fromJson(procedureClient.query("wordfreq",
                                                                     ImmutableMap.of("word", "text:testing")),
                                               resultType);

      Assert.assertEquals(100L, result.get("text:testing").longValue());

      // Verify by looking into dataset
      MyKeyValueTable mydataset = applicationManager.getDataSet("mydataset");

      Assert.assertEquals(100L, Longs.fromByteArray(mydataset.read("title:title".getBytes(Charsets.UTF_8))));

      // check the metrics
      RuntimeMetrics procedureMetrics = RuntimeStats.getProcedureMetrics("WordCountApp", "WordFrequency");
      procedureMetrics.waitForProcessed(1, 1, TimeUnit.SECONDS);
      Assert.assertEquals(0L, procedureMetrics.getException());

      // Run mapreduce job
      MapReduceManager mrManager = applicationManager.startMapReduce("countTotal");
      mrManager.waitForFinish(120L, TimeUnit.SECONDS);

      long totalCount = Long.valueOf(procedureClient.query("total", Collections.<String, String>emptyMap()));
      // every event has 5 tokens
      Assert.assertEquals(5 * 100L, totalCount);

    } finally {
      applicationManager.stopAll();
      clear();
    }
  }

  @Test
  public void testGenerator() throws InterruptedException, IOException, TimeoutException {
    ApplicationManager applicationManager = deployApplication(GenSinkApp2.class);

    try {
      applicationManager.startFlow("GenSinkFlow");

      // Check the flowlet metrics
      RuntimeMetrics genMetrics = RuntimeStats.getFlowletMetrics("GenSinkApp",
                                                                 "GenSinkFlow",
                                                                 "GenFlowlet");

      RuntimeMetrics sinkMetrics = RuntimeStats.getFlowletMetrics("GenSinkApp",
                                                                  "GenSinkFlow",
                                                                  "SinkFlowlet");
      sinkMetrics.waitForProcessed(99, 5, TimeUnit.SECONDS);
      Assert.assertEquals(0L, sinkMetrics.getException());

      Assert.assertEquals(1L, genMetrics.getException());

    } finally {
      applicationManager.stopAll();
      clear();
    }
  }
}
