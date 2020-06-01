/*
 * Copyright © 2019 Cask Data, Inc.
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
import io.cdap.cdap.api.annotation.Description;
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
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Distinct aggregator.
 */
@Plugin(type = BatchAggregator.PLUGIN_TYPE)
@Name("Distinct")
@Description("Deduplicates input records so that only provided fields are used to apply distinction on while other " +
  "fields are projected out.")
public class DistinctAggregator extends BatchAggregator<StructuredRecord, StructuredRecord, StructuredRecord> {
  public static final String NAME = "Distinct";
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Conf conf;
  private Iterable<String> fields;
  private Schema outputSchema;

  public DistinctAggregator(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();

    this.outputSchema = getOutputSchema(stageConfigurer.getFailureCollector(), inputSchema, conf.getFields());
    stageConfigurer.setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(BatchRuntimeContext context) {
    outputSchema = context.getOutputSchema();
    fields = conf.getFields();
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
  public void aggregate(StructuredRecord groupKey, Iterator<StructuredRecord> iterator,
                        Emitter<StructuredRecord> emitter) {
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
    return Schema.recordOf(inputSchema.getRecordName() + ".distinct", outputFields);
  }

  /**
   * Plugin Configuration.
   */
  public static class Conf extends PluginConfig {
    @Description("Comma-separated list of fields to perform the distinct on.")
    private String fields;

    Iterable<String> getFields() {
      return Splitter.on(',').trimResults().split(fields);
    }
  }

  public static ETLPlugin getPlugin(String field) {
    Map<String, String> properties = new HashMap<>();
    properties.put("fields", field);
    return new ETLPlugin(DistinctAggregator.NAME, BatchAggregator.PLUGIN_TYPE, properties);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("field", new PluginPropertyField("field", "", "string", true, false));
    return new PluginClass(BatchAggregator.PLUGIN_TYPE, "DistinctAggregator", "", DistinctAggregator.class.getName(),
                           "config", properties);
  }
}
