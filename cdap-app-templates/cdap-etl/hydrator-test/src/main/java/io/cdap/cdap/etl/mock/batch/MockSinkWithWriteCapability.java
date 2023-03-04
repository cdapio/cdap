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

package io.cdap.cdap.etl.mock.batch;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineOutput;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Mock sink that exposes direct write capabilities for the {@link MockSQLEngineWithCapabilities}
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(MockSinkWithWriteCapability.NAME)
public class MockSinkWithWriteCapability extends AbstractMockSink {

  public static final String NAME = "MockWithWriteCapability";
  public static final PluginClass PLUGIN_CLASS = getPluginClass();

  public MockSinkWithWriteCapability(Config config) {
    super(config);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    super.prepareRun(context);
    context.addOutput(new SQLEngineOutput(NAME,
        context.getStageName(),
        MockSQLEngineWithCapabilities.class.getName(),
        Collections.emptyMap()));
  }

  /**
   * Get the plugin config to be used in a pipeline config
   *
   * @param tableName the table name used to store results
   * @return the plugin config to be used in a pipeline config
   */
  public static ETLPlugin getPlugin(String tableName) {
    Map<String, String> properties = new HashMap<>();
    properties.put("tableName", tableName);
    return new ETLPlugin(NAME, BatchSink.PLUGIN_TYPE, properties, null);
  }

  /**
   * Get the plugin config to be used in a pipeline config based on the connection name
   *
   * @param connectionName the connection name backing the mock source
   * @return the plugin config to be used in a pipeline config
   */
  public static ETLPlugin getPluginUsingConnection(String connectionName) {
    Map<String, String> properties = new HashMap<>();
    properties.put("connectionConfig", String.format("${conn(%s)}", connectionName));
    return new ETLPlugin(NAME, BatchSink.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("tableName", new PluginPropertyField("tableName", "", "string", true, true));
    properties.put("connectionConfig",
        new PluginPropertyField("connectionConfig", "", "connectionconfig", true, true,
            false, Collections.singleton("tableName")));
    return PluginClass.builder().setName(NAME).setType(BatchSink.PLUGIN_TYPE)
        .setDescription("").setClassName(MockSinkWithWriteCapability.class.getName())
        .setProperties(properties)
        .setConfigFieldName("config").build();
  }
}
