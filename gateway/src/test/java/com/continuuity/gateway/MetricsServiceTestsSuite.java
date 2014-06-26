package com.continuuity.gateway;

import com.continuuity.gateway.handlers.log.LogHandlerTest;
import com.continuuity.gateway.handlers.metrics.MetricsDeleteTest;
import com.continuuity.gateway.handlers.metrics.MetricsDiscoveryQueryTest;
import com.continuuity.gateway.handlers.metrics.MetricsQueryTest;
import com.continuuity.gateway.handlers.metrics.MetricsSuiteTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test Suite for running all API tests.
 */
@RunWith(value = Suite.class)
@Suite.SuiteClasses(value = {
  MetricsQueryTest.class,
  MetricsDeleteTest.class,
  MetricsDiscoveryQueryTest.class,
  LogHandlerTest.class
})

public class MetricsServiceTestsSuite  {

  @BeforeClass
  public static void init() throws Exception {
    MetricsSuiteTestBase.beforeClass();
    MetricsSuiteTestBase.runBefore = false;
    MetricsSuiteTestBase.runAfter = false;
  }

  @AfterClass
  public static void finish() throws Exception {
    MetricsSuiteTestBase.runAfter = true;
    MetricsSuiteTestBase.afterClass();
  }
}
