package com.continuuity.examples.logger;

import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *  Test AccessLogApp
 */
public class AccessLogTest extends ReactorTestBase {
  private static final Gson GSON = new Gson();

  @Test
  public void test() throws Exception {
    // Deploy an App
    ApplicationManager appManager = deployApplication(AccessLogApp.class);

    // Start a Flow
    FlowManager flowManager = appManager.startFlow("LogAnalyticsFlow");

    long now = System.currentTimeMillis();

    try {
      sendData(appManager, now);

      // Wait for the last Flowlet processing 3 log events, or at most 5 seconds
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("AccessLogAnalytics","LogAnalyticsFlow", "counter");
      metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);
    } finally {
      flowManager.stop();
    }

    // Verify the processed data
    verifyCountProcedure(appManager);
  }

  /**
   * Send a few events to the Stream
   * @param appManager an ApplicationManager instance
   * @param now the current system time
   * @throws IOException
   */
  private void sendData(ApplicationManager appManager, long now)
    throws IOException {
    // Define a StreamWriter to send Apache log events in String to the App.
    StreamWriter streamWriter = appManager.getStreamWriter("logEventStream");

    streamWriter.send("1.202.218.8 - - [12/Apr/2012:02:13:43 -0400] " +
    "\"GET /robots.txt HTTP/1.0\" 404 208 \"-\" \"Mozilla/5.0\"");
    streamWriter.send("124.115.0.140 - - [12/Apr/2012:02:28:49 -0400] " +
    "\"GET / HTTP/1.1\" 200 392 \"-\" " +
    "\"Sosospider+(+http://help.soso.com/webspider.htm)\"");
    streamWriter.send("83.160.166.85 - - [12/Apr/2012:22:59:12 -0400] " +
    "\"GET /robots.txt HTTP/1.1\" 404 208 \"-\" \"portscout/0.8.1\"");
  }

  private void verifyCountProcedure(ApplicationManager appManager)
    throws IOException {
    // Start a Procedure
    ProcedureManager procedureManager = appManager.startProcedure(
                AccessLogApp.StatusCodeProcedure.class.getSimpleName());

    try {
      // Call the Procedure
      ProcedureClient client = procedureManager.getClient();

      // Verify the Procedure method get-counts. In this method, no runtime argument is required.
      String response = client.query("getCounts", Collections.<String, String>emptyMap());

      Map<Integer, Long> result = new Gson().fromJson(response,
                         new TypeToken<Map<Integer, Long>>(){}.getType());

      Assert.assertEquals(1, (long)result.get(200));
      Assert.assertEquals(2, (long)result.get(404));
    } finally {
      procedureManager.stop();
    }
  }
}
