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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.data2.dataset2.lib.file.FileSetModule;
import co.cask.cdap.data2.dataset2.lib.table.CubeModule;

/**
 * App with streams, datasets, plugins added multiple times that should result in an error.
 */
public class AppWithDuplicateData extends AbstractApplication<AppWithDuplicateData.ConfigClass> {

  public static class ConfigClass extends Config {
    private boolean multiDatasets;
    private boolean multiPlugins;
    private boolean multiModules;

    public ConfigClass() {
      multiDatasets = false;
      multiPlugins = false;
      multiModules = false;
    }

    public ConfigClass(boolean datasets, boolean plugins, boolean multiModules) {
      this.multiDatasets = datasets;
      this.multiPlugins = plugins;
      this.multiModules = multiModules;
    }
  }

  @Override
  public void configure() {
    ConfigClass config = getConfig();

    if (config.multiDatasets) {
      createDataset("data1", Table.class);
    }

    if (config.multiPlugins) {
      usePlugin("t1", "n1", "plug", PluginProperties.builder().build());
    }

    if (config.multiModules) {
      addDatasetModule("module", CubeModule.class);
    }
    addWorker(new DumbWorker());
  }

  public static class DumbWorker extends AbstractWorker {

    @Override
    public void run() {
      // no-op
    }

    @Override
    protected void configure() {
      super.configure();
      createDataset("data1", KeyValueTable.class);
      addDatasetModule("module", FileSetModule.class);
      usePlugin("t1", "n1", "plug", PluginProperties.builder().build());
    }
  }
}
