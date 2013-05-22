package com.continuuity.performance.test.app;

import com.continuuity.api.Application;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.performance.application.ApplicationPerformanceTestBase;
import com.continuuity.performance.application.BenchmarkRuntimeMetrics;
import com.continuuity.performance.application.BenchmarkRuntimeStats;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.StreamWriter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *  Example of application-level benchmark.
 */
public class SimplePerformanceTest extends ApplicationPerformanceTestBase {

  private final Class<? extends Application> appClass = SimpleApp.class;
  private final String appName = appClass.getSimpleName();
  private final int numStreamEvents = 40000;

  public SimplePerformanceTest(String accountId, CConfiguration config) {
    super(accountId, config);
  }

  public void testApp() throws IOException, TimeoutException, InterruptedException {

    clearAppFabric();

    ApplicationManager appMgr = deployApplication(appClass);

    try {

      FlowManager flowMgr = appMgr.startFlow("SimpleApp");

      flowMgr.setFlowletInstances("source", 2);

      StreamWriter kvStream = appMgr.getStreamWriter("keyValues");

      for (int i = 0; i < numStreamEvents; i++) {
        kvStream.send("key" + i + "=" + "val" + i);
      }

      BenchmarkRuntimeMetrics sourceFlowletMetrics = BenchmarkRuntimeStats.getFlowletMetrics(appName, "SimpleApp",
                                                                                             "source");

      sourceFlowletMetrics.waitForProcessed(numStreamEvents, 30, TimeUnit.SECONDS);

      BenchmarkRuntimeMetrics readerFlowletMetrics = BenchmarkRuntimeStats.getFlowletMetrics(appName, "SimpleApp",
                                                                                             "reader");

      readerFlowletMetrics.waitForProcessed(numStreamEvents, 30, TimeUnit.SECONDS);

    } finally {

      appMgr.stopAll();
    }
  }

  public static void main(String[] args) throws InterruptedException, TimeoutException, IOException {
    CConfiguration config = CConfiguration.create();
    config.set("zk", "db101.ubench.sl");
    config.set("host", "db101.ubench.sl");
    SimplePerformanceTest perfTest = new SimplePerformanceTest("developer", config);
    perfTest.testApp();
  }
}

