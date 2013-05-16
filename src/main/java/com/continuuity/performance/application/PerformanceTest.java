package com.continuuity.performance.application;

import com.continuuity.metrics2.thrift.Counter;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.StreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *  Example of unit-test like performance test.
 */
public class PerformanceTest extends AppFabricBenchmarkBase {

  private static final Logger LOG = LoggerFactory.getLogger(PerformanceTest.class);

  public void testApp() {

    clearAppFabric();

    ApplicationManager bam = deployApplication("com.continuuity.examples.wordcount.WordCount");

    try {
      LOG.debug("Starting flow WordCounter...");
      FlowManager flowMgr = bam.startFlow("WordCounter");

      LOG.debug("Starting to ingest data into stream wordStream...");

      StreamWriter s1 = bam.getStreamWriter("wordStream");

      for (int i = 0; i < 2000; i++) {
        s1.send("Saaa-sha!");
        s1.send("Aaaa-lex!");
      }

      BenchmarkRuntimeMetrics splitterMetrics = BenchmarkRuntimeStats.getFlowletMetrics("WordCount", "WordCounter",
                                                                                        "splitter");
      long splitterProcessed = splitterMetrics.getProcessed();
      System.out.println("Events processed so far by flowlet splitter = " + splitterProcessed);

      splitterMetrics.waitForProcessed(4000, 10, TimeUnit.SECONDS);

      splitterProcessed = splitterMetrics.getProcessed();
      System.out.println("Events processed so far by flowlet splitter = " + splitterProcessed);

      Counter meanReadRate = BenchmarkRuntimeStats.getCounter("WordCount", "WordCounter", "splitter",
                                                              "tuples.read.meanRate");

      Map<String, Double> splitterCounters = BenchmarkRuntimeStats.getCounters("WordCount", "WordCounter", "splitter");

      Map<String, Double> retrieveCounters = BenchmarkRuntimeStats.getCounters("WordCount", "RetrieveCount");

      Map<String, Double> allCounters = BenchmarkRuntimeStats.getCounters("WordCount", "WordCounter");


      System.out.println("Events processed so far by flowlet splitter = " + splitterMetrics.getProcessed());
      System.out.println("Events processed so far by flowlet associator = "
        + BenchmarkRuntimeStats.getFlowletMetrics("WordCount", "WordCounter", "associator").getProcessed());
      System.out.println("Events processed so far by flowlet counter = "
        + BenchmarkRuntimeStats.getFlowletMetrics("WordCount", "WordCounter", "counter").getProcessed());
      System.out.println("Events processed so far by flowlet unique = "
        + BenchmarkRuntimeStats.getFlowletMetrics("WordCount", "WordCounter", "unique").getProcessed());

      System.out.println("Saaa-sha is happy - hopefully :)");
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      bam.stopAll();
//      clearAppFabric();
    }
  }

  public static void main(String[] args) {
    PerformanceTest.init();
    PerformanceTest rtft = new PerformanceTest();
    rtft.testApp();
  }
}

