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

package co.cask.cdap.test.app;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.XSlowTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 *
 */
@Category(SlowTests.class)
public class DatasetUpgradeDisabledTest extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(
    Constants.Dataset.DATASET_UNCHECKED_UPGRADE, false,
    Constants.Explore.EXPLORE_ENABLED, false
  );

  @Category(XSlowTests.class)
  @Test
  public void testDatasetUncheckedUpgrade() throws Exception {
    deployApplication(DatasetUncheckedUpgradeApp.class);
    DataSetManager<DatasetUncheckedUpgradeApp.RecordDataset> datasetManager =
      getDataset(DatasetUncheckedUpgradeApp.DATASET_NAME);
    DatasetUncheckedUpgradeApp.Record expectedRecord = new DatasetUncheckedUpgradeApp.Record("0AXB", "john", "doe");
    datasetManager.get().writeRecord("key", expectedRecord);
    datasetManager.flush();

    DatasetUncheckedUpgradeApp.Record actualRecord =
      (DatasetUncheckedUpgradeApp.Record) datasetManager.get().getRecord("key");
    Assert.assertEquals(expectedRecord, actualRecord);

    // Test incompatible upgrade
    deployApplication(IncompatibleDatasetUncheckedUpgradeApp.class);
    datasetManager = getDataset(DatasetUncheckedUpgradeApp.DATASET_NAME);
    // new dataset is incompatible, but because dataset upgrade is disabled, it should not have an effect
    // this would either fail in getRecord() or throw a class cast exception if the dataset had been upgraded
    DatasetUncheckedUpgradeApp.Record compatibleRecord =
      (DatasetUncheckedUpgradeApp.Record) datasetManager.get().getRecord("key");
    Assert.assertEquals(expectedRecord, compatibleRecord);
  }
}
