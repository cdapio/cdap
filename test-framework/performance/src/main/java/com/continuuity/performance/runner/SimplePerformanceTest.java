/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.runner;

import com.continuuity.performance.application.BenchmarkRuntimeMetrics;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.StreamWriter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *  Example of application-level performance test.
 */
public class SimplePerformanceTest {

  @PerformanceTest
  public void testApp() throws IOException, TimeoutException, InterruptedException {
    final int numStreamEvents = 1000;

    ApplicationManager applicationManager = PerformanceTestRunner.deployApplication(
      "com.continuuity.performance.apps.simple.SimpleApp");

    try {

      FlowManager flowManager = applicationManager.startFlow("SimpleFlow");

      BenchmarkRuntimeMetrics sourceFlowletMetrics = BenchmarkRuntimeStats.getFlowletMetrics("SimpleApp", "SimpleFlow",
                                                                                             "source");
      final long initialSourceFlowletMetric = sourceFlowletMetrics.getProcessed();

      System.out.println(String.format("Number of events previously processed by source flowlet = %d",
                                       sourceFlowletMetrics.getProcessed()));

      StreamWriter kvStream = applicationManager.getStreamWriter("SimpleStream");

      for (int i = 0; i < numStreamEvents; i++) {
        kvStream.send(String.format("key%d=val%d", i, i));
      }


      System.out.println(String.format("Number of events processed by source flowlet = %d",
                                       sourceFlowletMetrics.getProcessed()));

      sourceFlowletMetrics.waitForProcessed(initialSourceFlowletMetric + numStreamEvents, 180, TimeUnit.SECONDS);

      System.out.println(String.format("Number of events processed by source flowlet = %d",
                                       sourceFlowletMetrics.getProcessed()));

      BenchmarkRuntimeMetrics readerFlowletMetrics = BenchmarkRuntimeStats.getFlowletMetrics("SimpleApp", "SimpleFlow",
                                                                                             "reader");

      System.out.println(String.format("Number of events processed by reader flowlet = %d",
                                       readerFlowletMetrics.getProcessed()));

      readerFlowletMetrics.waitForProcessed(numStreamEvents, 180, TimeUnit.SECONDS);

      System.out.println(String.format("Number of events processed by reader flowlet = %d",
                                       readerFlowletMetrics.getProcessed()));

      flowManager.stop();

    } finally {
      applicationManager.stopAll();
    }
  }
}
