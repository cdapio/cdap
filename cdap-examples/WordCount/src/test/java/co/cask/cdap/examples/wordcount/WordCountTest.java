/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.examples.wordcount;

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Word Count main test.
 */
public class WordCountTest extends TestBase {

  static Type stringMapType = new TypeToken<Map<String, String>>() {
  }.getType();
  static Type objectMapType = new TypeToken<Map<String, Object>>() {
  }.getType();

  @Test
  public void testWordCount() throws IOException, TimeoutException, InterruptedException {
    // Deploy the Application
    ApplicationManager appManager = deployApplication(WordCount.class);

    // Start the Flow
    appManager.startFlow("WordCounter");

    // Send a few events to the stream
    StreamWriter writer = appManager.getStreamWriter("wordStream");
    writer.send("hello world");
    writer.send("a wonderful world");
    writer.send("the world says hello");

    // Wait for the events to be processed, or at most 5 seconds
    RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("WordCount", "WordCounter", "associator");
    metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);

    // Start RetrieveCounts service
    ServiceManager serviceManager = appManager.startService(RetrieveCounts.SERVICE_NAME);

    // Wait service startup
    serviceStatusCheck(serviceManager, true);

    // First verify global statistics
    String response = requestService(new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "stats"));
    Map<String, String> map = new Gson().fromJson(response, stringMapType);
    Assert.assertEquals("9", map.get("totalWords"));
    Assert.assertEquals("6", map.get("uniqueWords"));
    Assert.assertEquals(((double) 42) / 9, Double.valueOf(map.get("averageLength")), 0.001);

    // Now verify statistics for a specific word
    response = requestService(new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "count/world"));
    Map<String, Object> omap = new Gson().fromJson(response, objectMapType);
    Assert.assertEquals("world", omap.get("word"));
    Assert.assertEquals(3.0, omap.get("count"));

    // The associations are a map within the map
    @SuppressWarnings("unchecked")
    Map<String, Double> assocs = (Map<String, Double>) omap.get("assocs");
    Assert.assertEquals(2.0, assocs.get("hello"), 0.000001);
    Assert.assertTrue(assocs.containsKey("hello"));

    appManager.stopAll();
    TimeUnit.SECONDS.sleep(1);
    clear();
  }

  private String requestService(URL url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    try {
      return new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
    } finally {
      conn.disconnect();
    }
  }

  private void serviceStatusCheck(ServiceManager serviceManger, boolean running) throws InterruptedException {
    int trial = 0;
    while (trial++ < 5) {
      if (serviceManger.isRunning() == running) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    throw new IllegalStateException("Service state not executed. Expected " + running);
  }
}
