/**
 * Copyright 2013-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.examples.sentiment;

import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.ProcedureManager;
import com.continuuity.test.ReactorTestBase;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
      ApplicationManager appManager = deployApplication(SentimentAnalysisApp.class);

      // Starts a Flow
      FlowManager flowManager = appManager.startFlow("analysis");

      try {
        // Write a message to Stream
        StreamWriter streamWriter = appManager.getStreamWriter("sentence");
        streamWriter.send("i love movie");
        streamWriter.send("i hate movie");
        streamWriter.send("i am neutral to movie");
        streamWriter.send("i am happy today that I got this working.");

        // Wait for the last Flowlet processed all tokens
        RuntimeMetrics countMetrics = RuntimeStats.getFlowletMetrics("sentiment", "analysis", "update");
        countMetrics.waitForProcessed(4, 15, TimeUnit.SECONDS);
      } finally {
        flowManager.stop();
      }

      // Start procedure and verify
      ProcedureManager procedureManager = appManager.startProcedure("sentiment-query");
      try {
        String response = procedureManager.getClient().query("aggregates", Collections.<String, String>emptyMap());

        // Verify the aggregates
        Map<String, Long> result = new Gson().fromJson(response, new TypeToken<Map<String, Long>>() { }.getType());
        Assert.assertEquals(2, result.get("positive").intValue());
        Assert.assertEquals(1, result.get("negative").intValue());
        Assert.assertEquals(1, result.get("neutral").intValue());

        // Verify retrieval of sentiments
        response = procedureManager.getClient().query("sentiments", ImmutableMap.of("sentiment", "positive"));
        result = new Gson().fromJson(response, new TypeToken<Map<String, Long>>() { }.getType());
        Assert.assertEquals(ImmutableSet.of("i love movie", "i am happy today that I got this working."),
                            result.keySet());

        response = procedureManager.getClient().query("sentiments", ImmutableMap.of("sentiment", "negative"));
        result = new Gson().fromJson(response, new TypeToken<Map<String, Long>>() { }.getType());
        Assert.assertEquals(ImmutableSet.of("i hate movie"), result.keySet());

        response = procedureManager.getClient().query("sentiments", ImmutableMap.of("sentiment", "neutral"));
        result = new Gson().fromJson(response, new TypeToken<Map<String, Long>>() { }.getType());
        Assert.assertEquals(ImmutableSet.of("i am neutral to movie"), result.keySet());
      } finally {
        procedureManager.stop();
      }
    } finally {
      TimeUnit.SECONDS.sleep(1);
      clear();
    }
  }

}
