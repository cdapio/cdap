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

package co.cask.cdap.spark.metrics;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.XSlowTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

/**
 * Test Spark program metrics.
 */
@Category(XSlowTests.class)
public class TestSparkMetricsIntegration extends TestBase {

  public static final String METRICS_KEY = ".BlockManager.memory.remainingMem_MB";

  @Test
  public void testSparkMetrics() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestSparkMetricsIntegrationApp.class);
    try {
      SparkManager sparkManager = applicationManager.startSpark(TestSparkMetricsIntegrationApp.APP_SPARK_NAME);
      sparkManager.waitForFinish(120, TimeUnit.SECONDS);

      Assert.assertTrue(RuntimeStats.getSparkMetrics(Constants.DEFAULT_NAMESPACE,
                                                     TestSparkMetricsIntegrationApp.APP_NAME,
                                                     TestSparkMetricsIntegrationApp.APP_SPARK_NAME, METRICS_KEY) > 0);
      //TODO: Add test to check user metrics once the support is added: CDAP-765
    } finally {
      applicationManager.stopAll();
      TimeUnit.SECONDS.sleep(1);
      clear();
    }
  }
}
