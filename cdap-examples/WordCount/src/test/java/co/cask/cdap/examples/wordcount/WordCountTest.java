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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Word Count main test.
 */
public class WordCountTest extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type OBJECT_MAP_TYPE = new TypeToken<Map<String, Object>>() { }.getType();

  @Test
  public void testWordCount() throws Exception {

    WordCount.WordCountConfig config = new WordCount.WordCountConfig("words", "stats", "counts", "unique", "assoc");
    // Deploy the Application
    ApplicationManager appManager = deployApplication(WordCount.class, config);

    // validate that the wordCount table is empty, and that it has no entry for "world"
    DataSetManager<KeyValueTable> datasetManager = getDataset(config.getWordCountTable());
    KeyValueTable wordCounts = datasetManager.get();
    Assert.assertNull(wordCounts.read("world"));

    // Start the Flow
    FlowManager flowManager = appManager.getFlowManager("WordCounter").start();

    // Send a few events to the stream
    StreamManager streamManager = getStreamManager("words");
    streamManager.send("hello world");
    streamManager.send("a wonderful world");
    streamManager.send("the world says hello");

    // Wait for the events to be processed, or at most 5 seconds
    RuntimeMetrics metrics = flowManager.getFlowletMetrics("associator");
    metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);

    // start a new transaction so that the "wordCounts" dataset sees changes made by the flow
    datasetManager.flush();
    Assert.assertEquals(3L, Bytes.toLong(wordCounts.read("world")));

    // Start RetrieveCounts service
    ServiceManager serviceManager = appManager.getServiceManager(RetrieveCounts.SERVICE_NAME).start();

    // Wait service startup
    serviceManager.waitForStatus(true);

    // First verify global statistics
    String response = requestService(new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "stats"));
    Map<String, String> map = new Gson().fromJson(response, STRING_MAP_TYPE);
    Assert.assertEquals("9", map.get("totalWords"));
    Assert.assertEquals("6", map.get("uniqueWords"));
    Assert.assertEquals(((double) 42) / 9, Double.valueOf(map.get("averageLength")), 0.001);

    // Now verify statistics for a specific word
    response = requestService(new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "count/world"));
    Map<String, Object> omap = new Gson().fromJson(response, OBJECT_MAP_TYPE);
    Assert.assertEquals("world", omap.get("word"));
    Assert.assertEquals(3.0, omap.get("count"));

    // The associations are a map within the map
    @SuppressWarnings("unchecked")
    Map<String, Double> assocs = (Map<String, Double>) omap.get("assocs");
    Assert.assertEquals(2.0, assocs.get("hello"), 0.000001);
    Assert.assertTrue(assocs.containsKey("hello"));
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
}
