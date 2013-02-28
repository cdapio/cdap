package com.continuuity.test.app;

import com.continuuity.test.RuntimeMetrics;
import com.continuuity.internal.test.RuntimeStats;
import com.continuuity.test.AppFabricTestBase;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.ProcedureClient;
import com.continuuity.test.ProcedureManager;
import com.continuuity.test.StreamWriter;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class TestFrameworkTest extends AppFabricTestBase {

  @Test
  public void test() throws InterruptedException, IOException, TimeoutException {
    ApplicationManager applicationManager = deployApplication(WordCountApp2.class);

    try {
      applicationManager.startFlow("WordCountFlow");

      // Send some inputs to streams
      StreamWriter streamWriter = applicationManager.getStreamWriter("text");
      for (int i = 0; i < 100; i++) {
        streamWriter.send("testing message " + i);
      }

      // Check the flowlet metrics
      RuntimeMetrics flowletMetrics = RuntimeStats.getFlowletMetrics("WordCountApp",
                                                                     "WordCountFlow",
                                                                     "CountByField");
      flowletMetrics.waitForProcessed(300, 5, TimeUnit.SECONDS);
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

      // check the metrics
      RuntimeMetrics procedureMetrics = RuntimeStats.getProcedureMetrics("WordCountApp", "WordFrequency");
      procedureMetrics.waitForProcessed(1, 1, TimeUnit.SECONDS);
      Assert.assertEquals(0L, procedureMetrics.getException());

    } finally {
      applicationManager.stopAll();
    }
  }
}
