package com.continuuity.performance.runner;

import com.continuuity.performance.application.BenchmarkRuntimeMetrics;
import com.continuuity.performance.apps.simple.SimpleApp;
import com.continuuity.test.app.FlowManager;
import com.continuuity.test.app.StreamWriter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *  Example of application-level performance test.
 */
@RunWithApps({SimpleApp.class})
public class SimplePerformanceTest {

  @PerformanceTest
  public void testApp() throws IOException, TimeoutException, InterruptedException {
    final int numStreamEvents = 100000;

    FlowManager flowManager = PerformanceTestRunner.Context.startFlow("SimpleApp", "SimpleApp");

    flowManager.setFlowletInstances("source", 2);

    StreamWriter kvStream = PerformanceTestRunner.Context.getStreamWriter("SimpleApp", "keyValues");

    for (int i = 0; i < numStreamEvents; i++) {
      kvStream.send("key" + i + "=" + "val" + i);
    }

    BenchmarkRuntimeMetrics sourceFlowletMetrics =
      PerformanceTestRunner.Context.getFlowletMetrics("SimpleApp", "SimpleApp", "source");

    System.out.println("Number of events processed by source flowlet = " + sourceFlowletMetrics.getProcessed());

    sourceFlowletMetrics.waitForProcessed(numStreamEvents, 120, TimeUnit.SECONDS);

    System.out.println("Number of events processed by source flowlet = " + sourceFlowletMetrics.getProcessed());

    BenchmarkRuntimeMetrics readerFlowletMetrics =
      PerformanceTestRunner.Context.getFlowletMetrics("SimpleApp", "SimpleApp", "reader");

    System.out.println("Number of events processed by reader flowlet = " + readerFlowletMetrics.getProcessed());

    readerFlowletMetrics.waitForProcessed(numStreamEvents, 120, TimeUnit.SECONDS);

    System.out.println("Number of events processed by reader flowlet = " + readerFlowletMetrics.getProcessed());
  }
}

