/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.test.base;

import io.cdap.cdap.admin.AdminAppTestRun;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.test.TestSuite;
import io.cdap.cdap.mapreduce.service.MapReduceServiceIntegrationTestRun;
import io.cdap.cdap.service.DynamicPluginServiceTestRun;
import io.cdap.cdap.service.FileUploadServiceTestRun;
import io.cdap.cdap.service.ServiceArtifactTestRun;
import io.cdap.cdap.service.ServiceLifeCycleTestRun;
import io.cdap.cdap.spark.SparkFileSetTestRun;
import io.cdap.cdap.spark.SparkStreamingTestRun;
import io.cdap.cdap.spark.metrics.SparkMetricsIntegrationTestRun;
import io.cdap.cdap.spark.service.SparkServiceIntegrationTestRun;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.XSlowTests;
import io.cdap.cdap.test.app.TestFrameworkTestRun;
import io.cdap.cdap.test.messaging.MessagingAppTestRun;
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
  DynamicPluginServiceTestRun.class,
  FileUploadServiceTestRun.class,
  MapReduceServiceIntegrationTestRun.class,
  MessagingAppTestRun.class,
  ServiceArtifactTestRun.class,
  ServiceLifeCycleTestRun.class,
  SparkFileSetTestRun.class,
  SparkMetricsIntegrationTestRun.class,
  SparkServiceIntegrationTestRun.class,
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
