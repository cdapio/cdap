package com.continuuity.performance.test.app;

import com.continuuity.api.Application;
import com.continuuity.performance.application.AppFabricBenchmarkBase;
import com.continuuity.performance.application.BenchmarkRuntimeMetrics;
import com.continuuity.performance.application.BenchmarkRuntimeStats;
import com.continuuity.performance.application.MensaMetricsReporter;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.StreamWriter;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *  Example of application-level performance test.
 */
public class WriteAndReadPerformanceTest extends AppFabricBenchmarkBase {

  private static final Logger LOG = LoggerFactory.getLogger(WriteAndReadPerformanceTest.class);

  final Class<? extends Application> appClass = WriteAndRead.class;
  final String appName = appClass.getSimpleName();
  final String flowName = "WriteAndRead";

  public void testApp() {
    LOG.info("Clearing AppFabric...");
    clearAppFabric();

    LOG.info("Deploying application '{}'...", appName);
    ApplicationManager bam = deployApplication(appClass);

    LOG.info("Starting mensa metrics reporter...");

    MensaMetricsReporter mmr =
      new MensaMetricsReporter("mon101.ops.sl", 4242,
                               ImmutableList.of("developer:WriteAndRead:WriteAndRead:source:processed.count"), "", 10);

    try {
      LOG.info("Starting flow '{}'...", flowName);
      FlowManager flowMgr = bam.startFlow(flowName);

      LOG.info("Starting to ingest data into stream keyValues...");

      StreamWriter kvStream = bam.getStreamWriter("keyValues");

      for (int i = 0; i < 40000; i++) {
        kvStream.send("key" + i + "=" + "val" + i);
      }

      Map<String, Double> allCounters = BenchmarkRuntimeStats.getCounters(appName, flowName);

      BenchmarkRuntimeMetrics sourceMetrics = BenchmarkRuntimeStats.getFlowletMetrics(appName, flowName, "source");

      logProcessed();

      try {
        sourceMetrics.waitForProcessed(10000, 30, TimeUnit.SECONDS);
      } catch (TimeoutException te) {
        LOG.warn("Ignoring TimeoutException");
      }

      logProcessed();

      BenchmarkRuntimeMetrics readerMetrics = BenchmarkRuntimeStats.getFlowletMetrics(appName, flowName, "reader");

      logProcessed();

      readerMetrics.waitForProcessed(10000, 30, TimeUnit.SECONDS);

      logProcessed();

      mmr.reportNow("dataset.storage.writeAndRead.count", 1000.0);

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      LOG.info("Shutting down mensa metrics reporter...");
      mmr.shutdown();

      LOG.info("Stopping application manager...");
      bam.stopAll();

      LOG.info("Clearing AppFabric...");
      clearAppFabric();
    }
  }
  private void logProcessed() {
    String[] flowlets = {"source", "writer", "reader"};
    for (String flowlet : flowlets) {
      LOG.info("Events processed so far by flowlet '" + flowlet + "' = "
                 + BenchmarkRuntimeStats.getFlowletMetrics(appName, flowName, flowlet).getProcessed());
    }
  }

  public static void main(String[] args) {
    WriteAndReadPerformanceTest.init();
    WriteAndReadPerformanceTest perfTest = new WriteAndReadPerformanceTest();
    perfTest.testApp();
  }
}

