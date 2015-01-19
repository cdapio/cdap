/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.spark.service;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.XSlowTests;
import com.google.common.base.Throwables;
import org.apache.commons.io.Charsets;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Test Spark program integration with Service
 */
@Category(XSlowTests.class)
public class TestSparkServiceIntegration extends TestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestSparkServiceIntegration.class);

  @Test
  public void testSparkWithService() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestSparkServiceIntegrationApp.class);
    try {
      startService(applicationManager);

      SparkManager sparkManager = applicationManager.startSpark(
        TestSparkServiceIntegrationApp.SparkServiceProgram.class.getSimpleName());
      sparkManager.waitForFinish(120, TimeUnit.SECONDS);

      DataSetManager<KeyValueTable> datasetManager = applicationManager.getDataSet("result");
      KeyValueTable results = datasetManager.get();
      for (int i = 1; i <= 5; i++) {
        byte[] key = String.valueOf(i).getBytes(Charsets.UTF_8);
        Assert.assertEquals((i * i), Integer.parseInt(Bytes.toString(results.read(key))));
      }
    } finally {
      applicationManager.stopAll();
      TimeUnit.SECONDS.sleep(1);
      clear();
    }
  }

  /**
   * Starts a Service
   */
  private void startService(ApplicationManager applicationManager) {
    ServiceManager serviceManager = applicationManager.startService(TestSparkServiceIntegrationApp.SERVICE_NAME);
    try {
      serviceManager.waitForStatus(true);
    } catch (InterruptedException e) {
      LOG.error("Failed to start {} service", TestSparkServiceIntegrationApp.SERVICE_NAME, e);
      throw Throwables.propagate(e);
    }
  }
}
