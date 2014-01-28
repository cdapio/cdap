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
    // Deploy an app.
    ApplicationManager appManager = deployApplication(AccessLogApp.class);

    // Start a flow by passing the name if the name is specified in metadata.
    FlowManager flowManager = appManager.startFlow("log-analytics-flow");

    long now = System.currentTimeMillis();

    try {
      sendData(appManager, now);

      // Wait for the last flowlet processing 3 events, or at most 5 seconds.
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("accesslog",
                                              "log-analytics-flow", "counter");
      metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);
    } finally {
      flowManager.stop();
    }

    // Verify data processed well
    verifyCountProcedure(appManager);
  }

  /**
   * Send a few events to the stream.
   * @param appManager
   * @param now
   * @throws IOException
   */
  private void sendData(ApplicationManager appManager, long now)
    throws IOException {
    // Define a StreamWriter to send Apache log events in String to the App.
    StreamWriter streamWriter = appManager.getStreamWriter("log-events");

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
    // Start a procedure by passing the class name if the name is not
    // specified in metadata.
    ProcedureManager procedureManager = appManager.startProcedure(
                AccessLogApp.LogProcedure.class.getSimpleName());

    try {
      // Call the procedure
      ProcedureClient client = procedureManager.getClient();

      // Verify the query get-all-counts.
      // Perform a query by passing the handler name and parameters.
      // Parameters are passed by a map of parameters to values. In case of
      // success, the response is a Json string. There is no parameter
      // required for get-all-counts query.
      String response = client.query("get-counts",
                                     Collections.<String, String>emptyMap());
      // Deserialize the Json string.
      Map<Integer, Long> result = new Gson().fromJson(response,
                         new TypeToken<Map<Integer, Long>>(){}.getType());
      Assert.assertEquals(1, (long)result.get(200));
      Assert.assertEquals(2, (long)result.get(404));
    } finally {
      procedureManager.stop();
    }
  }
}
