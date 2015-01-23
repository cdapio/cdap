/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.test.base;

import co.cask.cdap.batch.stream.BatchStreamIntegrationTestRun;
import co.cask.cdap.flow.stream.FlowStreamIntegrationTestRun;
import co.cask.cdap.mapreduce.MapReduceStreamInputTestRun;
import co.cask.cdap.mapreduce.service.MapReduceServiceIntegrationTestRun;
import co.cask.cdap.spark.metrics.SparkMetricsIntegrationTestRun;
import co.cask.cdap.spark.service.SparkServiceIntegrationTestRun;
import co.cask.cdap.spark.stream.SparkStreamIntegrationTestRun;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.app.DummyBaseCloneTestRun;
import co.cask.cdap.test.app.DummyBaseTestRun;
import co.cask.cdap.test.app.TestFrameworkTestRun;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * This is a test suite that runs all tests in the cdap-unit-test. This avoid starting/stopping app-fabric per test.
 */
@Category(XSlowTests.class)
@RunWith(Suite.class)
@Suite.SuiteClasses({
  BatchStreamIntegrationTestRun.class,
  DummyBaseTestRun.class,
  DummyBaseCloneTestRun.class,
  FlowStreamIntegrationTestRun.class,
  MapReduceStreamInputTestRun.class,
  MapReduceServiceIntegrationTestRun.class,
  SparkMetricsIntegrationTestRun.class,
  SparkServiceIntegrationTestRun.class,
  SparkStreamIntegrationTestRun.class,
  TestFrameworkTestRun.class
})
public class TestFrameworkTestSuite extends TestFrameworkTestBase {
}
