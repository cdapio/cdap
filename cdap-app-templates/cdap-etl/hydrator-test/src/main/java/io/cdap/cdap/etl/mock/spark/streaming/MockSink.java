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

package io.cdap.cdap.etl.mock.spark.streaming;

import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.test.DataSetManager;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Mock Sink for Spark streaming test.
 */
public class MockSink extends SparkSink<StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;

  public MockSink(Config config) {
    this.config = config;
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
    if (!context.datasetExists(config.tableName)) {
      context.createDataset(config.tableName, KeyValueTable.class.getName(), DatasetProperties.EMPTY);
    }
  }

  @Override
  public void run(final SparkExecutionPluginContext context, JavaRDD<StructuredRecord> input) throws Exception {
    JavaPairRDD<byte[], byte[]> tableData = input.mapToPair(new PairFunction<StructuredRecord, byte[], byte[]>() {
      @Override
      public Tuple2<byte[], byte[]> call(StructuredRecord record) throws Exception {
        return new Tuple2<>(Bytes.toBytes((String) record.get("id")), Bytes.toBytes((String) record.get("name")));
      }
    });

    context.saveAsDataset(tableData, config.tableName);
  }


  /**
   * Config for the sink.
   */
  public static class Config extends PluginConfig {
    @Macro
    private String tableName;
  }

  /**
   * Get ETL plugin
   * @param tableName name of the table provided for the configuration
   * @return an instance of the ETLPlugin
   */
  public static ETLPlugin getPlugin(String tableName) {
    Map<String, String> properties = new HashMap<>();
    properties.put("tableName", tableName);
    return new ETLPlugin("Mock", SparkSink.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("tableName", new PluginPropertyField("tableName", "", "string", true, true));
    return PluginClass.builder().setName("Mock").setType(SparkSink.PLUGIN_TYPE)
             .setDescription("").setClassName(MockSink.class.getName()).setProperties(properties)
             .setConfigFieldName("config").build();
  }

  /**
   * Get the values associated with the specified keys.
   * @param keys keys for which value to be determined
   * @param tableManager manager for the table
   * @return the key value map
   * @throws Exception
   */
  public static Map<String, String> getValues(Set<String> keys, DataSetManager<KeyValueTable> tableManager)
    throws Exception {
    tableManager.flush();
    KeyValueTable table = tableManager.get();

    Map<String, String> values = new HashMap<>();
    for (String key : keys) {
      values.put(key, Bytes.toString(table.read(key)));
    }
    return values;
  }
}
