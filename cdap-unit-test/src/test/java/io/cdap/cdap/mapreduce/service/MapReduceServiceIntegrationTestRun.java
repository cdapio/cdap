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

import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.app.MyKeyValueTableDefinition;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class MapReduceServiceIntegrationTestRun extends TestFrameworkTestBase {

  @Test
  public void test() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestMapReduceServiceIntegrationApp.class);
    ServiceManager serviceManager =
      applicationManager.getServiceManager(TestMapReduceServiceIntegrationApp.SERVICE_NAME).start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    DataSetManager<MyKeyValueTableDefinition.KeyValueTable> inDataSet =
      getDataset(TestMapReduceServiceIntegrationApp.INPUT_DATASET);
    inDataSet.get().write("key1", "Two words");
    inDataSet.get().write("key2", "Plus three words");
    inDataSet.flush();

    MapReduceManager mrManager =
      applicationManager.getMapReduceManager(TestMapReduceServiceIntegrationApp.MR_NAME).start();
    mrManager.waitForRun(ProgramRunStatus.COMPLETED, 180, TimeUnit.SECONDS);

    DataSetManager<MyKeyValueTableDefinition.KeyValueTable> outDataSet =
      getDataset(TestMapReduceServiceIntegrationApp.OUTPUT_DATASET);
    MyKeyValueTableDefinition.KeyValueTable results = outDataSet.get();

    String total = results.get(TestMapReduceServiceIntegrationApp.SQUARED_TOTAL_WORDS_COUNT);
    Assert.assertEquals(25, Integer.parseInt(total));
  }
}
