/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.continuuity;

import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.ProcedureManager;
import com.continuuity.test.ReactorTestBase;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
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

      try {
        // Write a message to stream
        StreamWriter streamWriter = appManager.getStreamWriter("text");
        streamWriter.send("i love movie");
        streamWriter.send("i am happy today that I got this working.");
        streamWriter.send("i hate movie");
        streamWriter.send("i am neutral to movie");

        // Wait for the last flowlet processed all tokens.
        RuntimeMetrics countMetrics = RuntimeStats.getFlowletMetrics("sentiment", "analysis", "update");
        countMetrics.waitForProcessed(4, 10, TimeUnit.SECONDS);
      } finally {
        flowManager.stop();
      }

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
      TimeUnit.SECONDS.sleep(1);
      clear();
    }
  }

}
