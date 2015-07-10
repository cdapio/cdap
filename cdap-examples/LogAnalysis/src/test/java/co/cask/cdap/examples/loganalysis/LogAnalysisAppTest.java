/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.examples.loganalysis;

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link LogAnalysisApp}
 */
public class LogAnalysisAppTest extends TestBase {
  private static final String LOG_1 = "127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] \"GET /home.html HTTP/1.1\" 200 2048";
  private static final String LOG_2 = "127.0.1.1 - - [21/Jul/2014:9:55:27 -0800] \"GET /home.html HTTP/1.1\" 400 2048";
  private static final String LOG_3 = "127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] \"GET /index.html HTTP/1.1\" 200 2048";

  private static final String TOTAL_HITS_VALUE = "2";
  private static final String TOTAL_RESPONSE_VALUE = "2";
  private static final String RESPONSE_CODE = "200";

  @Test
  public void test() throws Exception {
    // Deploy the App
    ApplicationManager appManager = deployApplication(LogAnalysisApp.class);

    // Send a stream events to the Stream
    StreamManager streamManager = getStreamManager(LogAnalysisApp.LOG_STREAM);
    streamManager.send(LOG_1);
    streamManager.send(LOG_2);
    streamManager.send(LOG_3);

    // run the spark program
    SparkManager sparkManager = appManager.getSparkManager(ResponseCounterProgram.class.getSimpleName()).start();
    sparkManager.waitForFinish(60, TimeUnit.SECONDS);

    // run the mapreduce job
    MapReduceManager mapReduceManager = appManager.getMapReduceManager(HitCounterProgram.class.getSimpleName()).start();
    mapReduceManager.waitForFinish(3, TimeUnit.MINUTES);

    // start and wait for services
    ServiceManager hitCounterServiceManager = getServiceManager(appManager, LogAnalysisApp.HIT_COUNTER_SERVICE);
    ServiceManager responseCounterServiceManager = getServiceManager(appManager,
                                                                     LogAnalysisApp.RESPONSE_COUNTER_SERVICE);

    //Query for hit counts and verify it
    URL totalHitsURL = new URL(hitCounterServiceManager.getServiceURL(15, TimeUnit.SECONDS),
                               LogAnalysisApp.HitCounterServiceHandler.HIT_COUNTER_SERVICE_PATH);
    HttpURLConnection totalHitsURLConnection = (HttpURLConnection) totalHitsURL.openConnection();

    try {
      totalHitsURLConnection.setDoOutput(true);
      totalHitsURLConnection.setRequestMethod("POST");
      totalHitsURLConnection.getOutputStream().write(("{\"url\":\"" + "/home.html" + "\"}").getBytes(Charsets.UTF_8));

      Assert.assertEquals(HttpURLConnection.HTTP_OK, totalHitsURLConnection.getResponseCode());

      if (totalHitsURLConnection.getResponseCode() == HttpURLConnection.HTTP_OK) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(totalHitsURLConnection.getInputStream()));
        Assert.assertEquals(TOTAL_HITS_VALUE, reader.readLine());
      }
    } finally {
      totalHitsURLConnection.disconnect();
    }

    // query for total responses for a response code and verify it
    String response = requestService(new URL(responseCounterServiceManager.getServiceURL(15, TimeUnit.SECONDS),
                                             LogAnalysisApp.ResponseCounterHandler.RESPONSE_COUNT_PATH
                                               + "/" + RESPONSE_CODE));
    Assert.assertEquals(TOTAL_RESPONSE_VALUE, response);
  }

  private ServiceManager getServiceManager(ApplicationManager appManager, String serviceName)
    throws InterruptedException {
    // Start the service
    ServiceManager serviceManager =
      appManager.getServiceManager(serviceName).start();

    // Wait for service startup
    serviceManager.waitForStatus(true);
    return serviceManager;
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
