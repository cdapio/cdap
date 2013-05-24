package com.continuuity.cperf.runner;

import com.continuuity.performance.application.BenchmarkRuntimeMetrics;
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

  @Test
  public void testApp() throws IOException, TimeoutException, InterruptedException {
    final int numStreamEvents = 10000;

    FlowManager flowMgr = Renn.Leitung.startFlow("SimpleApp", "SimpleFlow");

    flowMgr.setFlowletInstances("source", 2);

    StreamWriter kvStream = Renn.Leitung.getStreamWriter("SimpleApp", "keyValues");

    for (int i = 0; i < numStreamEvents; i++) {
      kvStream.send("key" + i + "=" + "val" + i);
    }

    BenchmarkRuntimeMetrics sourceFlowletMetrics = Renn.Leitung.getFlowletMetrics("SimpleApp", "SimpleFlow", "source");

    System.out.println("Number of events processed by source flowlet = " + sourceFlowletMetrics.getProcessed());

    sourceFlowletMetrics.waitForProcessed(numStreamEvents, 120, TimeUnit.SECONDS);

    System.out.println("Number of events processed by source flowlet = " + sourceFlowletMetrics.getProcessed());

    BenchmarkRuntimeMetrics readerFlowletMetrics = Renn.Leitung.getFlowletMetrics("SimpleApp", "SimpleFlow", "reader");

    System.out.println("Number of events processed by reader flowlet = " + readerFlowletMetrics.getProcessed());

    readerFlowletMetrics.waitForProcessed(numStreamEvents, 120, TimeUnit.SECONDS);

    System.out.println("Number of events processed by reader flowlet = " + readerFlowletMetrics.getProcessed());
  }
}

