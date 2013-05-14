package com.continuuity.performance.application;

import com.continuuity.performance.benchmark.StreamRunner;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.performance.RemoteAppFabricTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample class for unit-test like performance test.
 */
public class PerformanceTest extends RemoteAppFabricTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(PerformanceTest.class);

  public void testApp() {

    clearAppFabric();

    ApplicationManager applicationManager = deployApplication("com.continuuity.examples.wordcount.WordCount");

    try {
      LOG.debug("Starting flow WordCounter...");
      applicationManager.startFlow("WordCounter");

      LOG.debug("Starting to ingest data into stream wordStream...");

      StreamRunner sr = new StreamRunner("wordStream");
      sr.run(10);

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      applicationManager.stopAll();
      clearAppFabric();
    }
  }

  public static void main(String[] args) {
    PerformanceTest.init();
    PerformanceTest rtft = new PerformanceTest();
    rtft.testApp();
  }
}

