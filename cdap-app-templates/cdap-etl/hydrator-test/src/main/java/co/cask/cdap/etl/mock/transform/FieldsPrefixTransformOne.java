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

package co.cask.cdap.etl.mock.transform;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.proto.v2.ETLPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("FieldsPrefixTransformOne")
public class FieldsPrefixTransformOne extends Transform<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private static final Schema outSchema = Schema.recordOf(
    "join.output",
    Schema.Field.of("tTwoid", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("tTwoname", Schema.of(Schema.Type.STRING))
  );

  private final Config config;

  public FieldsPrefixTransformOne(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    try {
//      Schema outSchema = config.getOutputSchema(Schema.parseJson(config.schemaStr));
      stageConfigurer.setOutputSchema(outSchema);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid output schema: " + e.getMessage(), e);
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {

    StructuredRecord joinRecordSamuel = StructuredRecord.builder(outSchema).
      set("tTwoname", "samuel").set("tTwoid", "1").build();
    emitter.emit(joinRecordSamuel);
    StructuredRecord joinRecordManuel = StructuredRecord.builder(outSchema).
      set("tTwoname", "manuel").set("tTwoid", "1").build();
    emitter.emit(joinRecordManuel);
    StructuredRecord joinRecordBob = StructuredRecord.builder(outSchema).
      set("tTwoname", "bob").set("tTwoid", "2").build();
    emitter.emit(joinRecordBob);
  }

  /**
   * Config for join plugin
   */
  public static class Config extends PluginConfig {
    private String prefix;
    private String schemaStr;

    public Config() {
      prefix = "prefix";
      schemaStr = "schemaStr";
    }

    private Schema getOutputSchema(Schema inputSchema) {
      List<Schema.Field> outFields = new ArrayList<>();
      List<Schema.Field> fields = inputSchema.getFields();
      for (Schema.Field field : fields) {
        outFields.add(Schema.Field.of(prefix + field.getName(), field.getSchema()));
      }
      return Schema.recordOf("prefixed.outfields", outFields);
    }
  }

  public static ETLPlugin getPlugin(String prefix, String schemaStr) {
    Map<String, String> properties = new HashMap<>();
    properties.put("prefix", prefix);
    properties.put("schemaStr", schemaStr);
    return new ETLPlugin("FieldsPrefixTransformOne", Transform.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("prefix", new PluginPropertyField("prefix", "", "string", true));
    properties.put("schemaStr", new PluginPropertyField("schemaStr", "", "string", true));
    return new PluginClass(Transform.PLUGIN_TYPE, "FieldsPrefixTransformOne", "", FieldsPrefixTransform.class.getName(),
                           "config", properties);
  }
}
