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
package com.continuuity.examples.traffic;

import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.MapReduceManager;
import com.continuuity.test.ProcedureClient;
import com.continuuity.test.ProcedureManager;
import com.continuuity.test.ReactorTestBase;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *  Test TrafficAnalyticsApp
 */
public class TrafficAnalyticsTest extends ReactorTestBase {
  private static final Gson GSON = new Gson();
  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
  private static final long NOW = System.currentTimeMillis();
  private static final long AGGREGATION_INTERVAL = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);

  @Test
  public void test() throws Exception {
    
    // Deploy an Application.
    ApplicationManager appManager = deployApplication(TrafficAnalyticsApp.class);

    // Start a Flow.
    FlowManager flowManager = appManager.startFlow("RequestCountFlow");
    MapReduceManager mrManager = null;

    long now = System.currentTimeMillis();

    try {
      sendData(appManager, now);

      // Wait for the last Flowlet processing 3 events, or at most 5 seconds.
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("TrafficAnalytics", "RequestCountFlow", "collector");
      metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);

      // Start a MapReduce job.
      mrManager = appManager.startMapReduce("RequestCountMapReduce");
      mrManager.waitForFinish(60, TimeUnit.SECONDS);
    } finally {
      flowManager.stop();
    }

    // Verify data processed well.
    verifyCountProcedure(appManager);
  }

  /**
   * Send a few events to the stream.
   * @param appManager
   * @param now
   * @throws IOException
   */
  private void sendData(ApplicationManager appManager, long now) throws IOException {
    
    // Define a StreamWriter to send Apache log events.
    StreamWriter streamWriter = appManager.getStreamWriter("logEventStream");

    streamWriter.send("1.202.218.8 - - [" + DATE_FORMAT.format(new Date(NOW)) + "] " +
    "\"GET /robots.txt HTTP/1.0\" 404 208 \"-\" \"Mozilla/5.0\"");
    streamWriter.send("1.202.218.8 - - [" + DATE_FORMAT.format(new Date(NOW)) + "] " +
    "\"GET / HTTP/1.1\" 200 392 \"-\" " +
    "\"Sosospider+(+http://help.soso.com/webspider.htm)\"");

    long oneHourBefore = NOW - TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
    streamWriter.send("83.160.166.85 - - [" + DATE_FORMAT.format(new Date(oneHourBefore)) + "] " +
    "\"GET /robots.txt HTTP/1.1\" 404 208 \"-\" \"portscout/0.8.1\"");

    // Send a log event out of the time range.
    long twentyFiveHourBefore = NOW - TimeUnit.MILLISECONDS.convert(25, TimeUnit.HOURS);
    streamWriter.send("89.190.166.85 - - [" + DATE_FORMAT.format(new Date(twentyFiveHourBefore)) + "] " +
                        "\"GET /news.txt HTTP/1.1\" 404 208 \"-\" \"portscout/0.8.1\"");
  }

  private void verifyCountProcedure(ApplicationManager appManager)
    throws IOException {
      
    // Start a Procedure.
    ProcedureManager procedureManager = appManager.startProcedure(
      TrafficAnalyticsApp.LogCountProcedure.class.getSimpleName());

    try {
      // Call the Procedure
      ProcedureClient client = procedureManager.getClient();

      // Verify the query get-counts.
      String response = client.query("getCounts", Collections.<String, String>emptyMap());
      
      // Deserialize the Json string.
      Map<Long, Integer> result = GSON.fromJson(response, new TypeToken<Map<Long, Integer>>() { }.getType());
      Long nowByHour = NOW - NOW % AGGREGATION_INTERVAL;
      Assert.assertEquals(2, result.size());
      Assert.assertEquals(1, (int) result.get(nowByHour - TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS)));
      Assert.assertEquals(2, (int) result.get(nowByHour));
    } finally {
      procedureManager.stop();
    }
  }
}
