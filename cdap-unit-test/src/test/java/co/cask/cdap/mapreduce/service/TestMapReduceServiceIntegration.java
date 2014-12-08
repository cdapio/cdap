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

package co.cask.cdap.mapreduce.service;

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.app.MyKeyValueTableDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TestMapReduceServiceIntegration extends TestBase {

  @Test
  public void test() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestMapReduceServiceIntegrationApp.class);
    try {
      ServiceManager serviceManager = applicationManager.startService(TestMapReduceServiceIntegrationApp.SERVICE_NAME);
      serviceStatusCheck(serviceManager, true);

      DataSetManager<MyKeyValueTableDefinition.KeyValueTable> inDataSet =
        applicationManager.getDataSet(TestMapReduceServiceIntegrationApp.INPUT_DATASET);
      inDataSet.get().write("key1", "Two words");
      inDataSet.get().write("key2", "Plus three words");
      inDataSet.flush();

      MapReduceManager mrManager = applicationManager.startMapReduce(TestMapReduceServiceIntegrationApp.MR_NAME);
      mrManager.waitForFinish(180, TimeUnit.SECONDS);

      DataSetManager<MyKeyValueTableDefinition.KeyValueTable> outDataSet =
        applicationManager.getDataSet(TestMapReduceServiceIntegrationApp.OUTPUT_DATASET);
      MyKeyValueTableDefinition.KeyValueTable results = outDataSet.get();

      String total = results.get(TestMapReduceServiceIntegrationApp.SQUARED_TOTAL_WORDS_COUNT);
      Assert.assertEquals(25, Integer.parseInt(total));
    } finally {
      applicationManager.stopAll();
      TimeUnit.SECONDS.sleep(1);
      clear();
    }
  }

  private void serviceStatusCheck(ServiceManager serviceManger, boolean running) throws InterruptedException {
    int trial = 0;
    while (trial++ < 5) {
      if (serviceManger.isRunning() == running) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    throw new IllegalStateException("Service state not executed. Expected " + running);
  }
}
