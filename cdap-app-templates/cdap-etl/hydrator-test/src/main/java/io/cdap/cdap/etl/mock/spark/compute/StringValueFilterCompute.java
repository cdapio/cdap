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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.api.spark.dynamic.SparkInterpreter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.collection.JavaConversions;

import java.lang.reflect.Method;
import java.util.Collections;
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
  private SparkInterpreter interpreter;
  private Method computeMethod;

  public StringValueFilterCompute(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    // should never happen, here to test app correctness in unit tests
    Schema inputSchema = context.getInputSchema();
    if (inputSchema != null && !inputSchema.equals(context.getOutputSchema())) {
      throw new IllegalStateException("runtime schema does not match what was set at configure time.");
    }

    interpreter = context.createSparkInterpreter();
    interpreter.compile(
      "package test\n" +
      "import co.cask.cdap.api.data.format._\n" +
      "import org.apache.spark._\n" +
      "import org.apache.spark.api.java._\n" +
      "import org.apache.spark.rdd._\n" +
      "object Compute {\n" +
      "  def compute(rdd: RDD[StructuredRecord]): JavaRDD[StructuredRecord] = {\n" +
      "    val value = \"" + conf.value + "\"\n" +
      "    val field = \"" + conf.field + "\"\n" +
      "    JavaRDD.fromRDD(rdd.filter(r => !value.equals(r.get(field))))\n" +
      "  }\n" +
      "}"
    );

    computeMethod = interpreter.getClassLoader().loadClass("test.Compute").getDeclaredMethod("compute", RDD.class);
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> input) throws Exception {
    //noinspection unchecked
    return (JavaRDD<StructuredRecord>) computeMethod.invoke(null, input.rdd());
  }

  /**
   * Config for the plugin
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
