package com.continuuity.performance.test.app;

import com.continuuity.api.Application;
import com.continuuity.cperf.runner.Renn;
import com.continuuity.cperf.runner.RunWithApps;
import com.continuuity.performance.application.BenchmarkRuntimeMetrics;
import com.continuuity.performance.application.BenchmarkRuntimeStats;
import com.continuuity.test.FlowManager;
import com.continuuity.test.StreamWriter;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *  Example of application-level performance test.
 */
@RunWithApps({SimpleApp.class, TrivialApp.class})
public class SimpleRennen {

  private final Class<? extends Application> appClass = SimpleApp.class;
  private final String appName = appClass.getSimpleName();
  private final int numStreamEvents = 10000;

  @Test
  public void testApp() throws IOException, TimeoutException, InterruptedException {

    StreamWriter kvStream = null;
    try {

      FlowManager flowMgr = Renn.Leitung.startFlow(appName, "SimpleApp");

      flowMgr.setFlowletInstances("source", 2);

      kvStream = Renn.Leitung.getStreamWriter(appName, "keyValues");

      for (int i = 0; i < numStreamEvents; i++) {
        kvStream.send("key" + i + "=" + "val" + i);
      }

      BenchmarkRuntimeMetrics sourceFlowletMetrics = Renn.Leitung.getFlowletMetrics(appName, "SimpleApp", "source");

      sourceFlowletMetrics = BenchmarkRuntimeStats.getFlowletMetrics(appName, "SimpleApp",
                                                                                             "source");

      System.out.println("Number of events processed by source flowlet = " + sourceFlowletMetrics.getProcessed());

      sourceFlowletMetrics.waitForProcessed(numStreamEvents, 120, TimeUnit.SECONDS);

      System.out.println("Number of events processed by source flowlet = " + sourceFlowletMetrics.getProcessed());

      BenchmarkRuntimeMetrics readerFlowletMetrics = BenchmarkRuntimeStats.getFlowletMetrics(appName, "SimpleApp",
                                                                                             "reader");

      System.out.println("Number of events processed by reader flowlet = " + readerFlowletMetrics.getProcessed());

      readerFlowletMetrics.waitForProcessed(numStreamEvents, 120, TimeUnit.SECONDS);

      System.out.println("Number of events processed by reader flowlet = " + readerFlowletMetrics.getProcessed());

    } finally {
//      ((MultiThreadedStreamWriter) kvStream).shutdown();
//      appMgr.stopAll();
    }
  }

//  public static void main(String[] args) throws InterruptedException, TimeoutException, IOException {
//    CConfiguration config = CConfiguration.create();
//    config.set("zk", "db101.ubench.sl");
//    config.set("host", "db101.ubench.sl");
//    SimpleRennen perfTest = new SimpleRennen("developer", config);
//    perfTest.testApp();
//  }
}

