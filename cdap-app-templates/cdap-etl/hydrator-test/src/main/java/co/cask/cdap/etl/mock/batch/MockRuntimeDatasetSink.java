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

package co.cask.cdap.etl.mock.batch;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Map;

/**
 * Mock sink that tests creation of datasets during runtime based on if dataset exists or not.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("MockRuntime")
public class MockRuntimeDatasetSink extends MockSink {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();

  public MockRuntimeDatasetSink(Config config) {
    super(config);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    super.prepareRun(context);
    if (!context.datasetExists("mockRuntimeSinkDataset")) {
      context.createDataset("mockRuntimeSinkDataset", KeyValueTable.class.getName(), DatasetProperties.EMPTY);
    }
  }

  public static ETLPlugin getPlugin(String tableName) {
    Map<String, String> properties = new HashMap<>();
    properties.put("tableName", tableName);
    return new ETLPlugin("MockRuntime", BatchSink.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("tableName", new PluginPropertyField("tableName", "", "string", true, true));
    return new PluginClass(BatchSink.PLUGIN_TYPE, "MockRuntime", "", MockRuntimeDatasetSink.class.getName(),
                           "config", properties);
  }
}
