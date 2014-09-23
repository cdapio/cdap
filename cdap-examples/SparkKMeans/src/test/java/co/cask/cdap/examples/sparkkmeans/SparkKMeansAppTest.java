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

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ProcedureClient;
import co.cask.cdap.test.ProcedureManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test SparkKMeansApp
 */
public class SparkKMeansAppTest extends TestBase {

  private static final Gson GSON = new Gson();

  @Test(expected = IOException.class)
  public void test() throws Exception {
    // Deploy an Application
    ApplicationManager appManager = deployApplication(SparkKMeansApp.class);
    // Start a Flow
    FlowManager flowManager = appManager.startFlow("PointsFlow");
    try {
      // Inject data
      sendData(appManager);
      // Wait for the last Flowlet processing 3 events, or at most 5 seconds
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("SparkKMeans", "PointsFlow", "reader");
      metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);
      // Start a Spark Program
      SparkManager sparkManager = appManager.startSpark("SparkKMeansProgram");
      sparkManager.waitForFinish(60, TimeUnit.SECONDS);
    } finally {
      flowManager.stop();
    }
    // Verify data processed well
    verifyCentersProcedure(appManager);
  }

  /**
   * verify execution of the CentersProcedure
   */
  private void verifyCentersProcedure(ApplicationManager appManager) throws IOException, ParseException {
    ProcedureManager procedureManager = appManager.startProcedure(
      SparkKMeansApp.CentersProcedure.class.getSimpleName());
    try {
      ProcedureClient client = procedureManager.getClient();
      String response = client.query("centers", ImmutableMap.of("index", "1"));
      String centerCoordinates = GSON.fromJson(response, String.class);
      Assert.assertNotNull(centerCoordinates);
      String[] coordinates = centerCoordinates.split(",");
      Assert.assertTrue(coordinates.length == 3);
      for (String coordinate : coordinates) {
        double value = Double.parseDouble(coordinate);
        Assert.assertTrue(value > 0);
      }
      // test wrong argument
      client.query("centers", ImmutableMap.of("position", "0"));
      // test with invalid index
      client.query("centers", ImmutableMap.of("index", "5"));
    } finally {
      procedureManager.stop();
    }
  }

  /**
   * Send a few points to the stream
   */
  private void sendData(ApplicationManager appManager) throws IOException {
    StreamWriter streamWriter = appManager.getStreamWriter("pointsStream");
    streamWriter.send("0.0 0.0 0.0");
    streamWriter.send("0.1 0.1 0.1");
    streamWriter.send("0.2 0.2 0.2");
    streamWriter.send("9.0 9.0 9.0");
    streamWriter.send("9.1 9.1 9.1");
    streamWriter.send("9.2 9.2 9.2");
  }

}
