package com.continuuity;

import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.ProcedureManager;
import com.continuuity.test.ReactorTestBase;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SentimentAnalysisTest extends ReactorTestBase {

  @Test
  public void test() throws Exception {
    try {
      ApplicationManager appManager = deployApplication(SentimentAnalysis.class);

      // Starts a flow
      FlowManager flowManager = appManager.startFlow("analysis");

      // Write a message to stream
      StreamWriter streamWriter = appManager.getStreamWriter("text");
      streamWriter.send("i love movie");
      streamWriter.send("i am happy today that I got this working.");
      streamWriter.send("i hate movie");
      streamWriter.send("i am neutral to movie");

      // Wait for the last flowlet processed all tokens.
      RuntimeMetrics countMetrics = RuntimeStats.getFlowletMetrics("sentiment", "analysis", "update");
      countMetrics.waitForProcessed(4, 2, TimeUnit.SECONDS);

      flowManager.stop();

      // Start procedure and query for word frequency.
      ProcedureManager procedureManager = appManager.startProcedure("sentiment-query");
      try {
        String response = procedureManager.getClient().query("aggregates", Collections.<String, String>emptyMap());

        // Verify the frequency.
        Map<String, Long> result = new Gson().fromJson(response, new TypeToken<Map<String, Long>>(){}.getType());
        Assert.assertEquals(2, result.get("positive").intValue());
        Assert.assertEquals(1, result.get("negative").intValue());
        Assert.assertEquals(1, result.get("neutral").intValue());
      } finally {
        procedureManager.stop();
      }
    } finally {
      clear();
    }
  }

}
