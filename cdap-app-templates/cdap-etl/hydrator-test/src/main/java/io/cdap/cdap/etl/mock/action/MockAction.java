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

package io.cdap.cdap.etl.mock.action;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.test.DataSetManager;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Mock sink that writes records to a Table and has a utility method for getting all records written.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("TableWriterAction")
public class MockAction extends Action {
  private final Config config;
  public static final PluginClass PLUGIN_CLASS = getPluginClass();

  /**
   * Config for the MockAction
   */
  public static class Config extends PluginConfig {
    private String tableName;
    private String rowKey;
    private String columnKey;
    @Macro
    private String value;

    @Nullable
    private String argumentKey;
    @Nullable
    private String argumentValue;
  }

  public MockAction(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(config.tableName, Table.class);
  }

  @Override
  public void run(ActionContext context) throws Exception {
    context.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        Table table = context.getDataset(config.tableName);
        Put put = new Put(config.rowKey);
        put.add(config.columnKey, config.value);
        table.put(put);
      }
    });

    // Set the same value in the arguments as well.
    context.getArguments().set(config.rowKey + config.columnKey, config.value);

    if (config.argumentKey != null && config.argumentValue != null) {
      if (!context.getArguments().get(config.argumentKey).equals(config.argumentValue)) {
        throw new IllegalStateException(String.format("Expected %s to be present in the argument map with value %s.",
                                                      config.argumentKey, config.argumentValue));
      }
    }
  }

  public static ETLPlugin getPlugin(String tableName, String rowKey, String columnKey, String value,
                                    @Nullable String argumentKey, @Nullable String argumentValue) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("tableName", tableName);
    builder.put("rowKey", rowKey);
    builder.put("columnKey", columnKey);
    builder.put("value", value);
    if (argumentKey != null && argumentValue != null) {
      builder.put("argumentKey", argumentKey);
      builder.put("argumentValue", argumentValue);
    }
    return new ETLPlugin("TableWriterAction", Action.PLUGIN_TYPE, builder.build(), null);
  }

  public static ETLPlugin getPlugin(String tableName, String rowKey, String columnKey, String value) {
    return getPlugin(tableName, rowKey, columnKey, value, null, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("tableName", new PluginPropertyField("tableName", "", "string", true, false));
    properties.put("rowKey", new PluginPropertyField("rowKey", "", "string", true, false));
    properties.put("columnKey", new PluginPropertyField("columnKey", "", "string", true, false));
    properties.put("value", new PluginPropertyField("value", "", "string", true, true));
    properties.put("argumentKey", new PluginPropertyField("argumentKey", "", "string", false, false));
    properties.put("argumentValue", new PluginPropertyField("argumentValue", "", "string", false, false));
    return PluginClass.builder().setName("TableWriterAction").setType(Action.PLUGIN_TYPE)
             .setDescription("").setClassName(MockAction.class.getName()).setProperties(properties)
             .setConfigFieldName("config").build();
  }

  /**
   * Read the value for the specified rowKey and columnKey.
   */
  public static String readOutput(DataSetManager<Table> tableManager, String rowKey, String columnKey)
    throws Exception {
    Table table = tableManager.get();
    return Bytes.toString(table.get(Bytes.toBytes(rowKey), Bytes.toBytes(columnKey)));
  }
}
