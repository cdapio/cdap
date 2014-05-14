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
package com.continuuity.examples.wordcount;

import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.ProcedureClient;
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

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Word Count main test.
 */
public class WordCountTest extends ReactorTestBase {

  static Type stringMapType = new TypeToken<Map<String, String>>() {
  }.getType();
  static Type objectMapType = new TypeToken<Map<String, Object>>() {
  }.getType();

  @Test
  public void testWordCount() throws IOException, TimeoutException, InterruptedException {

    // Deploy the Application
    ApplicationManager appManager = deployApplication(WordCount.class);

    // Start the Flow and the Procedure
    FlowManager flowManager = appManager.startFlow("WordCounter");
    ProcedureManager procManager = appManager.startProcedure("RetrieveCounts");

    // Send a few events to the stream
    StreamWriter writer = appManager.getStreamWriter("wordStream");
    writer.send("hello world");
    writer.send("a wonderful world");
    writer.send("the world says hello");

    // Wait for the events to be processed, or at most 5 seconds
    RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("WordCount", "WordCounter", "associator");
    metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);

    // Now call the procedure
    ProcedureClient client = procManager.getClient();

    // First verify global statistics
    String response = client.query("getStats", Collections.EMPTY_MAP);
    Map<String, String> map = new Gson().fromJson(response, stringMapType);
    Assert.assertEquals("9", map.get("totalWords"));
    Assert.assertEquals("6", map.get("uniqueWords"));
    Assert.assertEquals(((double) 42) / 9, (double) Double.valueOf(map.get("averageLength")), 0.001);

    // Now verify some statistics for one of the words
    response = client.query("getCount", ImmutableMap.of("word", "world"));
    Map<String, Object> omap = new Gson().fromJson(response, objectMapType);
    Assert.assertEquals("world", omap.get("word"));
    Assert.assertEquals(3.0, omap.get("count"));

    // The associations are a map within the map
    @SuppressWarnings("unchecked")
    Map<String, Double> assocs = (Map<String, Double>) omap.get("assocs");
    Assert.assertEquals(2.0, (double) assocs.get("hello"), 0.000001);
    Assert.assertTrue(assocs.containsKey("hello"));
  }
}
