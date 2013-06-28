package com.continuuity.performance.runner;

import com.continuuity.performance.application.BenchmarkRuntimeMetrics;
import com.continuuity.performance.apps.simple.SimpleApp;
import com.continuuity.test.app.ApplicationManager;
import com.continuuity.test.app.FlowManager;
import com.continuuity.test.app.StreamWriter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *  Example of application-level performance test.
 */
public class SimplePerformanceTest {

  @PerformanceTest
  public void testApp() throws IOException, TimeoutException, InterruptedException {
    final int numStreamEvents = 10000;

    ApplicationManager applicationManager = PerformanceTestRunner.deployApplication(SimpleApp.class);

    try {

      FlowManager flowManager = applicationManager.startFlow("SimpleFlow");

      flowManager.setFlowletInstances("source", 2);

      StreamWriter kvStream = applicationManager.getStreamWriter("SimpleStream");

      for (int i = 0; i < numStreamEvents; i++) {
        kvStream.send("key" + i + "=" + "val" + i);
      }

      BenchmarkRuntimeMetrics sourceFlowletMetrics = BenchmarkRuntimeStats.getFlowletMetrics("SimpleApp", "SimpleFlow",
                                                                                             "source");

      System.out.println(String.format("Number of events processed by source flowlet = %d",
                                       sourceFlowletMetrics.getProcessed()));

      sourceFlowletMetrics.waitForProcessed(numStreamEvents, 120, TimeUnit.SECONDS);

      System.out.println(String.format("Number of events processed by source flowlet = %d",
                                       sourceFlowletMetrics.getProcessed()));

      BenchmarkRuntimeMetrics readerFlowletMetrics = BenchmarkRuntimeStats.getFlowletMetrics("SimpleApp", "SimpleFlow",
                                                                                             "reader");

      System.out.println(String.format("Number of events processed by reader flowlet = %d",
                                       readerFlowletMetrics.getProcessed()));

      readerFlowletMetrics.waitForProcessed(numStreamEvents, 120, TimeUnit.SECONDS);

      System.out.println(String.format("Number of events processed by reader flowlet = %d",
                                       readerFlowletMetrics.getProcessed()));

    } finally {
      applicationManager.stopAll();
    }
  }
}
