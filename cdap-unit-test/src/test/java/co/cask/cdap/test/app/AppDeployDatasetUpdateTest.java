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

package co.cask.cdap.test.app;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.app.CompatibleDatasetDeployApp.CompatibleRecord;
import co.cask.cdap.test.app.DatasetDeployApp.Record;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests an update of an app that defines a dataset type; tests both compatible and incompatible update.
 */
@Category(SlowTests.class)
public class AppDeployDatasetUpdateTest extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(
    Constants.Dataset.DATASET_UNCHECKED_UPGRADE, true,
    Constants.Explore.EXPLORE_ENABLED, false
  );

  @Category(XSlowTests.class)
  @Test
  public void testDatasetUncheckedUpgrade() throws Exception {
    Record expectedRecord = new Record("0AXB", "john", "doe");
    CompatibleRecord compatibleRecord = new CompatibleRecord("0AXB", "john", "doe");

    // deploy the app, get the dataset, write a record, retrieve and validate it
    deployApplication(DatasetDeployApp.class);
    DataSetManager<DatasetDeployApp.RecordDataset> datasetManager =
      getDataset(DatasetDeployApp.DATASET_NAME);
    Assert.assertEquals(Record.class.getName(), datasetManager.get().getRecordClassName());
    datasetManager.get().writeRecord("key", expectedRecord);
    datasetManager.flush();
    Assert.assertEquals(expectedRecord, datasetManager.get().getRecord("key"));

    // Test compatible upgrade: deploy compat app, retrieve record with new type, validate
    deployApplication(CompatibleDatasetDeployApp.class);
    datasetManager = getDataset(DatasetDeployApp.DATASET_NAME);
    Assert.assertEquals(CompatibleRecord.class.getName(), datasetManager.get().getRecordClassName());
    Assert.assertEquals(compatibleRecord, datasetManager.get().getRecord("key"));

    // Test incompatible upgrade: deploy incompatible app should fail
    try {
      deployApplication(IncompatibleDatasetDeployApp.class);
      Assert.fail("Expected to throw exception here due to an incompatible Dataset upgrade.");
    } catch (Exception e) {
      // Expected exception due to incompatible Dataset upgrade
    }
    // validate that the dataset is unchanged
    datasetManager = getDataset(DatasetDeployApp.DATASET_NAME);
    Assert.assertEquals(CompatibleRecord.class.getName(), datasetManager.get().getRecordClassName());
    Assert.assertEquals(compatibleRecord, datasetManager.get().getRecord("key"));

    // Test upgrade to an app that uses a different dataset module name ("other" instead of "record")
    try {
      deployApplication(ModuleConflictDatasetDeployApp.class);
      Assert.fail("Expected to throw exception here due to an incompatible Dataset module upgrade.");
    } catch (Exception e) {
      // Expected exception due to incompatible Dataset upgrade
    }
    // validate that the dataset is unchanged
    datasetManager = getDataset(DatasetDeployApp.DATASET_NAME);
    Assert.assertEquals(CompatibleRecord.class.getName(), datasetManager.get().getRecordClassName());
    Assert.assertEquals(compatibleRecord, datasetManager.get().getRecord("key"));
  }
}
