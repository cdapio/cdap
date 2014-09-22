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

package co.cask.cdap.examples.moviesteer;

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ProcedureClient;
import co.cask.cdap.test.ProcedureManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test MovieSteerApp
 */
public class MovieSteerAppTest extends TestBase {

  private static final Gson GSON = new Gson();

  @Test
  public void test() throws Exception {
    // Deploy an Application
    ApplicationManager appManager = deployApplication(MovieSteerApp.class);
    // Start a Flow
    FlowManager flowManager = appManager.startFlow("RatingsFlow");
    try {
      // Inject data
      sendData(appManager);
      // Wait for the last Flowlet processing 3 events, or at most 5 seconds
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("MovieSteer", "RatingsFlow", "reader");
      metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);
      // Start a Spark Program
      SparkManager sparkManager = appManager.startSpark("MovieSteerProgram");
      sparkManager.waitForFinish(60, TimeUnit.SECONDS);
    } finally {
      flowManager.stop();
    }
    // Verify data processed well
    verifyPredictionProcedure(appManager);
  }

  /**
   * verify execution of the PredictionProcedure
   */
  private void verifyPredictionProcedure(ApplicationManager appManager) throws IOException, ParseException {
    ProcedureManager procedureManager = appManager.startProcedure(
      MovieSteerApp.PredictionProcedure.class.getSimpleName());
    try {
      ProcedureClient client = procedureManager.getClient();
      Map<String, String> params = new HashMap<String, String>();
      params.put("userId", "1");
      params.put("movieId", "2");
      String response = client.query("getPrediction", params);
      String value = GSON.fromJson(response, String.class);
      Assert.assertNotNull(value);
      double prediction = Double.parseDouble(value);
      Assert.assertTrue(prediction > 0);
    } finally {
      procedureManager.stop();
    }
  }

  /**
   * Send a few ratings to the stream
   */
  private void sendData(ApplicationManager appManager) throws IOException {
    StreamWriter streamWriter = appManager.getStreamWriter("ratingsStream");
    streamWriter.send("0::2::3");
    streamWriter.send("0::3::1");
    streamWriter.send("0::5::2");
    streamWriter.send("1::2::2");
    streamWriter.send("1::3::1");
    streamWriter.send("1::4::2");
  }

}
