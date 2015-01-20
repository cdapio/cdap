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

package $package;

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * SparkKMeansApp main tests.
 */
public class SparkKMeansAppTest extends TestBase {

  @Test
  public void test() throws Exception {
    // Deploy the Application
    ApplicationManager appManager = deployApplication(SparkKMeansApp.class);

    // Start the Flow
    FlowManager flowManager = appManager.startFlow("PointsFlow");
    try {
      // Send a few points to the stream
      StreamWriter streamWriter = appManager.getStreamWriter("pointsStream");
      streamWriter.send("10.6 519.2 110.3");
      streamWriter.send("10.6 518.1 110.1");
      streamWriter.send("10.6 519.6 109.9");
      streamWriter.send("10.6 517.9 108.9");
      streamWriter.send("10.7 518 109.2");

      //  Wait for the events to be processed, or at most 5 seconds
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("SparkKMeans", "PointsFlow", "reader");
      metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);

      // Start a Spark Program
      SparkManager sparkManager = appManager.startSpark("SparkKMeansProgram");
      sparkManager.waitForFinish(60, TimeUnit.SECONDS);
    } finally {
      flowManager.stop();
    }

    // Start CentersService
    ServiceManager serviceManager = appManager.startService(SparkKMeansApp.CentersService.SERVICE_NAME);

    // Wait service startup
    serviceManager.waitForStatus(true);

    // Request data and verify it
    String response = requestService(new URL(serviceManager.getServiceURL(), "centers/1"));
    String[] coordinates = response.split(",");
    Assert.assertTrue(coordinates.length == 3);
    for (String coordinate : coordinates) {
      double value = Double.parseDouble(coordinate);
      Assert.assertTrue(value > 0);
    }

    // Request data by incorrect index and verify response
    URL url = new URL(serviceManager.getServiceURL(), "centers/10");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    try {
      Assert.assertEquals(HttpURLConnection.HTTP_NO_CONTENT, conn.getResponseCode());
    } finally {
      conn.disconnect();
    }

    appManager.stopAll();
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
