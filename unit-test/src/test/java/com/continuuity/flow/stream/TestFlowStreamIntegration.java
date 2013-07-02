package com.continuuity.flow.stream;

import com.continuuity.test.AppFabricTestBase;
import com.continuuity.test.app.ApplicationManager;
import com.continuuity.test.app.RuntimeMetrics;
import com.continuuity.test.app.RuntimeStats;
import com.continuuity.test.app.StreamWriter;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TestFlowStreamIntegration extends AppFabricTestBase {
  @Test
  public void testStreamBatch() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestFlowStreamIntegrationApp.class);
    try {
      StreamWriter s1 = applicationManager.getStreamWriter("s1");
      for (int i = 0; i < 50; i++) {
        s1.send(String.valueOf(i));
      }

      applicationManager.startFlow("StreamTestFlow");
      RuntimeMetrics flowletMetrics1 = RuntimeStats.getFlowletMetrics("TestFlowStreamIntegrationApp",
                                                                      "StreamTestFlow", "StreamReader");
      flowletMetrics1.waitForProcessed(1, 10, TimeUnit.SECONDS);
      if (flowletMetrics1.getException() > 0) {
        Assert.fail("StreamReader test failed");
      }
    } finally {
      applicationManager.stopAll();
      clearAppFabric();
    }
  }
}
