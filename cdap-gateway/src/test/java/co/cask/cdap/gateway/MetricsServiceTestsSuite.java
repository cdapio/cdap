/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.gateway;

import co.cask.cdap.gateway.handlers.log.LogHandlerTestRun;
import co.cask.cdap.gateway.handlers.metrics.MetricsDeleteTestRun;
import co.cask.cdap.gateway.handlers.metrics.MetricsDiscoveryQueryTestRun;
import co.cask.cdap.gateway.handlers.metrics.MetricsQueryTestRun;
import co.cask.cdap.gateway.handlers.metrics.MetricsSearchTestRun;
import co.cask.cdap.gateway.handlers.metrics.MetricsSuiteTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test Suite for running all API tests.
 */
@RunWith(value = Suite.class)
@Suite.SuiteClasses(value = {
  MetricsQueryTestRun.class,
  MetricsDeleteTestRun.class,
  MetricsDiscoveryQueryTestRun.class,
  MetricsSearchTestRun.class,
  LogHandlerTestRun.class
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
