/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap;

import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.worker.AbstractWorker;
import io.cdap.cdap.data2.dataset2.cache.TestDatasetDefinition;

/**
 * An app for testing the configuration for enabling/disabling custom dataset module deployment.
 */
public class AppWithCustomDatasetModule extends AbstractApplication {

  @Override
  public void configure() {
    addDatasetModule("test", TestDatasetModule.class);
    addWorker(new TestWorker());
  }

  /**
   * A custom dataset module for testing.
   */
  public static final class TestDatasetModule implements DatasetModule {

    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<KeyValueTable, DatasetAdmin> kvTableDef = registry.get("keyValueTable");
      TestDatasetDefinition definition = new TestDatasetDefinition("testDataset", kvTableDef);
      registry.add(definition);
    }
  }

  /**
   * A worker for testing.
   */
  public static final class TestWorker extends AbstractWorker {

    @Override
    public void run() {
      // no-op
    }
  }
}
