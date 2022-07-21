/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.mock.batch.aggregator;

import com.google.common.base.Splitter;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchAggregatorContext;
import io.cdap.cdap.etl.api.batch.BatchReducibleAggregator;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Distinct aggregator
 */
@Plugin(type = BatchReducibleAggregator.PLUGIN_TYPE)
@Name(DistinctReducibleAggregator.NAME)
public class DistinctReducibleAggregator
  extends BatchReducibleAggregator<StructuredRecord, StructuredRecord, StructuredRecord, StructuredRecord> {
  public static final String NAME = "DistinctReducibleAggregator";
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Conf config;
  private Iterable<String> fields;
  private Schema outputSchema;

  public DistinctReducibleAggregator(Conf config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();

    this.outputSchema = getOutputSchema(stageConfigurer.getFailureCollector(), inputSchema, config.getFields());
    stageConfigurer.setOutputSchema(outputSchema);
  }

  @Override
  public void prepareRun(BatchAggregatorContext context) {
    if (config.partitions != null) {
      context.setNumPartitions(config.partitions);
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) {
    outputSchema = context.getOutputSchema();
    fields = config.getFields();
  }

  @Override
  public void groupBy(StructuredRecord record, Emitter<StructuredRecord> emitter) {
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (String fieldName : fields) {
      builder.set(fieldName, record.get(fieldName));
    }
    emitter.emit(builder.build());
  }

  @Override
  public StructuredRecord initializeAggregateValue(StructuredRecord val) {
    return val;
  }

  @Override
  public StructuredRecord mergeValues(StructuredRecord aggValue, StructuredRecord groupValue) {
    return aggValue;
  }

  @Override
  public StructuredRecord mergePartitions(StructuredRecord value1, StructuredRecord value2) {
    return value1;
  }

  @Override
  public void finalize(StructuredRecord groupKey, StructuredRecord iterator, Emitter<StructuredRecord> emitter) {
    emitter.emit(groupKey);
  }

  private static Schema getOutputSchema(FailureCollector collector, Schema inputSchema, Iterable<String> fields) {
    List<Schema.Field> outputFields = new ArrayList<>();
    for (String fieldName : fields) {
      Schema.Field field = inputSchema.getField(fieldName);
      if (field == null) {
        collector.addFailure(String.format("Field %s does not exist in input schema.", fieldName),
                             String.format("Make sure field %s is present in the input schema", fieldName))
          .withConfigElement("fields", fieldName);
        continue;
      }
      outputFields.add(field);
    }
    collector.getOrThrowException();
    return Schema.recordOf("record", outputFields);
  }

  /**
   * Plugin Configuration.
   */
  public static class Conf extends PluginConfig {
    private String fields;

    @Nullable
    private Integer partitions;

    Iterable<String> getFields() {
      return Splitter.on(',').trimResults().split(fields);
    }
  }

  public static ETLPlugin getPlugin(String fields) {
    Map<String, String> properties = new HashMap<>();
    properties.put("fields", fields);
    return new ETLPlugin(NAME, BatchReducibleAggregator.PLUGIN_TYPE, properties);
  }

  public static ETLPlugin getPlugin(String fields, int partitions) {
    Map<String, String> properties = new HashMap<>();
    properties.put("fields", fields);
    properties.put("partitions", String.valueOf(partitions));
    return new ETLPlugin(NAME, BatchReducibleAggregator.PLUGIN_TYPE, properties);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("fields", new PluginPropertyField("fields", "", "string", true, false));
    properties.put("partitions", new PluginPropertyField("partitions", "", "int", false, false));
    return PluginClass.builder().setName(NAME).setType(BatchReducibleAggregator.PLUGIN_TYPE)
             .setDescription("").setClassName(DistinctReducibleAggregator.class.getName())
             .setConfigFieldName("config").setProperties(properties).build();
  }
}
