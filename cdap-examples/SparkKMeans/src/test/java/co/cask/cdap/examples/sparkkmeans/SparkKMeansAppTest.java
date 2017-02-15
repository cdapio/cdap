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

package co.cask.cdap.examples.sparkkmeans;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * SparkKMeansApp main tests.
 */
public class SparkKMeansAppTest extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @Test
  public void test() throws Exception {
    // Deploy the Application
    ApplicationManager appManager = deployApplication(SparkKMeansApp.class);
    // Start the Flow
    FlowManager flowManager = appManager.getFlowManager("PointsFlow").start();
    // Send a few points to the stream
    StreamManager streamManager = getStreamManager("pointsStream");

    // one cluster around (0, 500, 0) and another around (100, 0, 0)
    for (int i = 0; i < 100; i++) {
      double diff = Math.random() / 100;
      streamManager.send(String.format("%f %f %f", diff, 500 + diff, diff));
      streamManager.send(String.format("%f %f %f", 100 + diff, diff, diff));
    }

    //  Wait for the events to be processed, or at most 5 seconds
    RuntimeMetrics metrics = flowManager.getFlowletMetrics("reader");
    metrics.waitForProcessed(200, 10, TimeUnit.SECONDS);

    // Start a Spark Program
    SparkManager sparkManager = appManager.getSparkManager("SparkKMeansProgram").start();
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 60, TimeUnit.SECONDS);

    flowManager.stop();

    // Start CentersService
    ServiceManager serviceManager = appManager.getServiceManager(SparkKMeansApp.CentersService.SERVICE_NAME).start();

    // Wait service startup
    serviceManager.waitForStatus(true);

    // Request data and verify it
    String response = requestService(new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "centers/0"));
    String[] coordinates = response.split(",");
    int x0 = Double.valueOf(coordinates[0]).intValue();
    int y0 = Double.valueOf(coordinates[1]).intValue();
    int z0 = Double.valueOf(coordinates[2]).intValue();

    response = requestService(new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "centers/1"));
    coordinates = response.split(",");
    int x1 = Double.valueOf(coordinates[0]).intValue();
    int y1 = Double.valueOf(coordinates[1]).intValue();
    int z1 = Double.valueOf(coordinates[2]).intValue();

    // one cluster should be around (0, 500, 0) and the other around (100, 0, 0)
    if (x0 == 100) {
      Assert.assertEquals(0, y0);
      Assert.assertEquals(0, z0);
      Assert.assertEquals(0, x1);
      Assert.assertEquals(500, y1);
      Assert.assertEquals(0, z1);
    } else {
      Assert.assertEquals(0, x0);
      Assert.assertEquals(500, y0);
      Assert.assertEquals(0, z0);
      Assert.assertEquals(100, x1);
      Assert.assertEquals(0, y1);
      Assert.assertEquals(0, z1);
    }

    // Request data by incorrect index and verify response
    URL url = new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "centers/10");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    try {
      Assert.assertEquals(HttpURLConnection.HTTP_NO_CONTENT, conn.getResponseCode());
    } finally {
      conn.disconnect();
    }
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
