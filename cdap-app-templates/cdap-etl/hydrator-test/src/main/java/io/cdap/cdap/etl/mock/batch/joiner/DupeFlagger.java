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

package io.cdap.cdap.etl.mock.batch.joiner;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.JoinConfig;
import io.cdap.cdap.etl.api.JoinElement;
import io.cdap.cdap.etl.api.MultiInputPipelineConfigurer;
import io.cdap.cdap.etl.api.MultiInputStageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Plugin that takes exactly 2 inputs and flags records in one input if they also appear in the other.
 */
@Plugin(type = BatchJoiner.PLUGIN_TYPE)
@Name(DupeFlagger.NAME)
public class DupeFlagger extends BatchJoiner<StructuredRecord, StructuredRecord, StructuredRecord> {
  public static final String NAME = "DupeFlagger";
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;

  public DupeFlagger(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(MultiInputPipelineConfigurer pipelineConfigurer) {
    MultiInputStageConfigurer stageConfigurer = pipelineConfigurer.getMultiInputStageConfigurer();
    Map<String, Schema> inputSchemas = stageConfigurer.getInputSchemas();
    if (inputSchemas.size() != 2) {
      throw new IllegalArgumentException(String.format(
        "The DupeFlagger plugin must have exactly two inputs with the same schema, but found %d inputs.",
        inputSchemas.size()));
    }
    Iterator<Schema> schemaIterator = inputSchemas.values().iterator();
    Schema schema1 = schemaIterator.next();
    Schema schema2 = schemaIterator.next();
    if (!schema1.equals(schema2)) {
      throw new IllegalArgumentException("The DupeFlagger plugin must have exactly two inputs with the same schema, " +
                                           "but the schemas are not the same.");
    }
    if (!config.containsMacro("keep")) {
      if (!inputSchemas.keySet().contains(config.keep)) {
        throw new IllegalArgumentException(config.keep + " is not an input.");
      }
    }

    if (!config.containsMacro("flagField")) {
      stageConfigurer.setOutputSchema(getOutputSchema(schema1));
    }
  }

  private Schema getOutputSchema(Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.addAll(inputSchema.getFields());
    fields.add(Schema.Field.of(config.flagField, Schema.of(Schema.Type.BOOLEAN)));
    return Schema.recordOf(inputSchema.getRecordName() + ".flagged", fields);
  }

  @Override
  public StructuredRecord joinOn(String stageName, StructuredRecord record) throws Exception {
    return record;
  }

  @Override
  public JoinConfig getJoinConfig() {
    return new JoinConfig(Collections.singletonList(config.keep));
  }

  @Override
  public StructuredRecord merge(StructuredRecord joinKey, Iterable<JoinElement<StructuredRecord>> joinRow) {
    StructuredRecord record = null;
    boolean containsDupe = false;
    for (JoinElement<StructuredRecord> element : joinRow) {
      if (element.getStageName().equals(config.keep)) {
        record = element.getInputRecord();
      } else {
        containsDupe = true;
      }
    }
    if (record == null) {
      // can only happen if 'keep' was a macro and did not evaluate to one of the inputs
      throw new IllegalArgumentException("No record for " + config.keep + " was found.");
    }

    Schema outputSchema = getOutputSchema(record.getSchema());
    StructuredRecord.Builder outputBuilder = StructuredRecord.builder(outputSchema)
      .set(config.flagField, containsDupe);
    for (Schema.Field field : record.getSchema().getFields()) {
      outputBuilder.set(field.getName(), record.get(field.getName()));
    }
    return outputBuilder.build();
  }

  /**
   * Config for except plugin
   */
  public static class Config extends PluginConfig {
    @Macro
    @Description("input to keep")
    private final String keep;

    @Macro
    @Nullable
    @Description("name of the flag field")
    private final String flagField;

    public Config() {
      keep = null;
      flagField = "isDupe";
    }

    public Config(String keep, String flagField) {
      this.keep = keep;
      this.flagField = flagField;
    }
  }

  public static ETLPlugin getPlugin(String keep, String flagField) {
    Map<String, String> properties = new HashMap<>();
    properties.put("keep", keep);
    properties.put("flagField", flagField);
    return new ETLPlugin(DupeFlagger.NAME, BatchJoiner.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("keep", new PluginPropertyField("keep", "input to keep", "string", true, false));
    properties.put("flagField", new PluginPropertyField("flagField", "name of the flag field", "string", false, true));
    return PluginClass.builder().setName(NAME).setType(BatchJoiner.PLUGIN_TYPE)
             .setDescription("").setClassName(DupeFlagger.class.getName()).setProperties(properties)
             .setConfigFieldName("config").build();
  }
}
