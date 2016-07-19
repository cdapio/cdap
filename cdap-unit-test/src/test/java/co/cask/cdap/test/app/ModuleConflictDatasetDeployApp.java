/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetProperties;

/**
 * An app that is the same as DatasetDeployApp but uses a different name for its dataset module.
 */
public class ModuleConflictDatasetDeployApp extends DatasetDeployApp {

  @Override
  public void configure() {
    setName(NAME);
    addDatasetModule("other", RecordDatasetModule.class);
    createDataset(DATASET_NAME, RecordDataset.class,
                  DatasetProperties.builder().add("recordClassName", getRecordClass().getName()).build());
    addService("NoOpService", new NoOpHandler());
  }
}
