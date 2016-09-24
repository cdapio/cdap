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

package co.cask.cdap.etl.mock.spark.compute;

import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.util.HashMap;
import java.util.Map;

/**
 * Transform that filters out records whose configured field is a configured value.
 * For example, can filter all records whose 'foo' field is equal to 'bar'. Assumes the field is of type string.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("StringValueFilterCompute")
public class StringValueFilterCompute extends SparkCompute<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Conf conf;

  public StringValueFilterCompute(Conf conf) {
    this.conf = conf;
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    return input.filter(new Function<StructuredRecord, Boolean>() {
      @Override
      public Boolean call(StructuredRecord v1) throws Exception {
        return !conf.value.equals(v1.get(conf.field));
      }
    });
  }

  /**
   * Config for the plugin.
   */
  public static class Conf extends PluginConfig {
    @Macro
    private String field;

    @Macro
    private String value;
  }

  public static ETLPlugin getPlugin(String field, String value) {
    Map<String, String> properties = new HashMap<>();
    properties.put("field", field);
    properties.put("value", value);
    return new ETLPlugin("StringValueFilterCompute", SparkCompute.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("field", new PluginPropertyField("field", "", "string", true, true));
    properties.put("value", new PluginPropertyField("value", "", "string", true, true));
    return new PluginClass(SparkCompute.PLUGIN_TYPE, "StringValueFilterCompute", "",
                           StringValueFilterCompute.class.getName(),
                           "conf", properties);
  }
}
