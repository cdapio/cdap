/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.mock.condition;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.condition.Condition;
import co.cask.cdap.etl.api.condition.ConditionContext;
import co.cask.cdap.etl.api.condition.StageStatistics;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.test.DataSetManager;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Mock implementation of Condition in the pipeline.
 */
@Plugin(type = Condition.PLUGIN_TYPE)
@Name("Mock")
public class MockCondition extends Condition {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;

  public MockCondition(Config config) {
    this.config = config;
  }

  /**
   * Config for the sink.
   */
  public static class Config extends PluginConfig {
    private String name;
    private String tableName;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    if (config.tableName != null) {
      pipelineConfigurer.createDataset(config.tableName, Table.class);
    }
  }

  @Override
  public boolean apply(final ConditionContext context) throws Exception {
    String propertyName = config.name + ".branch.to.execute";
    String propertyValue = context.getArguments().get(propertyName);
    // write stage statistics if table name is provided
    if (config.tableName != null) {
      context.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext datasetContext) throws Exception {
          Table table = datasetContext.getDataset(config.tableName);
          for (Map.Entry<String, StageStatistics> entry : context.getStageStatistics().entrySet()) {
            String stageName = entry.getKey();
            StageStatistics statistics = entry.getValue();
            Put put = new Put("stats");
            put.add(stageName + ".input.records", String.valueOf(statistics.getInputRecordsCount()));
            put.add(stageName + ".output.records", String.valueOf(statistics.getOutputRecordsCount()));
            put.add(stageName + ".error.records", String.valueOf(statistics.getErrorRecordsCount()));
            table.put(put);
          }
        }
      });
    }
    return propertyValue != null && propertyValue.equals("true");
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("name", new PluginPropertyField("name", "", "string", true, false));
    properties.put("tableName", new PluginPropertyField("tableName", "", "string", false, false));
    return new PluginClass(Condition.PLUGIN_TYPE, "Mock", "", MockCondition.class.getName(), "config", properties);
  }

  public static ETLPlugin getPlugin(String name) {
    return getPlugin(name, null);
  }

  public static ETLPlugin getPlugin(String name, @Nullable String tableName) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("name", name);
    if (tableName != null) {
      builder.put("tableName", tableName);
    }
    Map<String, String> properties = builder.build();
    return new ETLPlugin("Mock", Condition.PLUGIN_TYPE, properties, null);
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
