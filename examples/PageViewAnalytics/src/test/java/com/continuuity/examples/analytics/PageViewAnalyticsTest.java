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
package com.continuuity.examples.analytics;

import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.ProcedureClient;
import com.continuuity.test.ProcedureManager;
import com.continuuity.test.ReactorTestBase;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *  Test PageViewAnalyticsApp
 */
public class PageViewAnalyticsTest extends ReactorTestBase {
  private static final Gson GSON = new Gson();
  private static final double OFF = 0.000001;

  @Test
  public void test() throws Exception {
    // Deploy an App
    ApplicationManager appManager = deployApplication(PageViewAnalyticsApp.class);

    // Start a Flow
    FlowManager flowManager = appManager.startFlow("PageViewFlow");

    long now = System.currentTimeMillis();

    try {
      sendData(appManager, now);

      // Wait for the last Flowlet processing 5 events, or at most 5 seconds
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("PageViewAnalytics", "PageViewFlow", "pageCount");
      metrics.waitForProcessed(5, 5, TimeUnit.SECONDS);
    } finally {
      flowManager.stop();
    }

    // Verify the processed data
    verifyCountProcedure(appManager);
  }

  /**
   * Send a few events to the Stream
   * @param appManager an ApplicationManger instance
   * @param now the current system time
   * @throws IOException
   */
  private void sendData(ApplicationManager appManager, long now)
    throws IOException {
    // Define a StreamWriter to send Apache log events in String format to the App
    StreamWriter streamWriter = appManager.getStreamWriter("logEventStream");

    streamWriter.send("1.202.218.8 - - [12/Apr/2012:02:13:43 -0400] " +
    "\"GET /product.html HTTP/1.0\" 404 208 \"http://www.continuuity.com\" \"Mozilla/5.0\"");
    streamWriter.send("124.115.0.140 - - [12/Apr/2012:02:28:49 -0400] " +
    "\"GET /product.html HTTP/1.1\" 200 392 \"http://www.continuuity.com\" " +
    "\"Sosospider+(+http://help.soso.com/webspider.htm)\"");
    streamWriter.send("83.160.166.85 - - [12/Apr/2012:22:59:12 -0400] " +
    "\"GET /career.html HTTP/1.1\" 404 208 \"http://www.continuuity.com\" \"portscout/0.8.1\"");
    streamWriter.send("1.202.218.8 - - [12/Apr/2012:02:13:43 -0400] " +
                        "\"GET /career.html HTTP/1.0\" 404 208 \"http://www.continuuity.com\" \"Mozilla/5.0\"");
    streamWriter.send("1.202.218.8 - - [12/Apr/2012:02:13:43 -0400] " +
                        "\"GET /career.html HTTP/1.0\" 404 208 \"http://www.continuuity.com\" \"Mozilla/5.0\"");
    // This log is not to be processed for the lack of referred URI. It is counted by the logs.noreferrer Metric.
    streamWriter.send("1.202.218.8 - - [12/Apr/2012:02:13:43 -0400] " +
                        "\"GET /career.html HTTP/1.0\" 404 208 \"-\" \"Mozilla/5.0\"");
  }

  private void verifyCountProcedure(ApplicationManager appManager)
    throws IOException {

    // Start a Procedure
    ProcedureManager procedureManager = appManager.startProcedure(
      PageViewAnalyticsApp.PageViewProcedure.class.getSimpleName());

    try {
      // Call the Procedure
      ProcedureClient client = procedureManager.getClient();

      // Verify get-dist by passing the page URI in a runtime argument {"page": "http://www.continuuity.com"}
      String response = client.query("getDistribution", ImmutableMap.of("page", "http://www.continuuity.com"));
      Map<String, Double> results = GSON.fromJson(response, new TypeToken<Map<String, Double>>() { }.getType());
      Assert.assertEquals(0.4, results.get("/product.html"), OFF);
      Assert.assertEquals(0.6, results.get("/career.html"), OFF);
    } finally {
      procedureManager.stop();
    }
  }
}
