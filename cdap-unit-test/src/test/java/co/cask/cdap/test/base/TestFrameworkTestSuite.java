/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.admin.AdminAppTestRun;
import co.cask.cdap.batch.stream.BatchStreamIntegrationTestRun;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.test.TestSuite;
import co.cask.cdap.flow.stream.FlowStreamIntegrationTestRun;
import co.cask.cdap.mapreduce.MapReduceStreamInputTestRun;
import co.cask.cdap.mapreduce.service.MapReduceServiceIntegrationTestRun;
import co.cask.cdap.service.FileUploadServiceTestRun;
import co.cask.cdap.spark.SparkFileSetTestRun;
import co.cask.cdap.spark.SparkStreamingTestRun;
import co.cask.cdap.spark.metrics.SparkMetricsIntegrationTestRun;
import co.cask.cdap.spark.service.SparkServiceIntegrationTestRun;
import co.cask.cdap.spark.stream.SparkStreamIntegrationTestRun;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.app.ServiceLifeCycleTestRun;
import co.cask.cdap.test.app.TestFrameworkTestRun;
import co.cask.cdap.test.messaging.MessagingAppTestRun;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * This is a test suite that runs all tests in the cdap-unit-test module that don't require explore to be enabled.
 * This avoid starting/stopping app-fabric per test.
 */
@Category(XSlowTests.class)
@RunWith(TestSuite.class)
@Suite.SuiteClasses({
  AdminAppTestRun.class,
  BatchStreamIntegrationTestRun.class,
  FileUploadServiceTestRun.class,
  FlowStreamIntegrationTestRun.class,
  MapReduceStreamInputTestRun.class,
  MapReduceServiceIntegrationTestRun.class,
  MessagingAppTestRun.class,
  ServiceLifeCycleTestRun.class,
  SparkFileSetTestRun.class,
  SparkMetricsIntegrationTestRun.class,
  SparkServiceIntegrationTestRun.class,
  SparkStreamIntegrationTestRun.class,
  TestFrameworkTestRun.class,
  SparkStreamingTestRun.class,
})
public class TestFrameworkTestSuite extends TestFrameworkTestBase {
  // Note that setting the following configuration in any of the above Test classes is ignored, since
  // they are run as part of this TestSuite.
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.CLUSTER_NAME, "testCluster");

}
