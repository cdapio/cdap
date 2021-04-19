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

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.JoinConfig;
import io.cdap.cdap.etl.api.JoinElement;
import io.cdap.cdap.etl.api.MultiInputPipelineConfigurer;
import io.cdap.cdap.etl.api.MultiInputStageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * Join plugin to perform joins on structured records
 */
@Plugin(type = BatchJoiner.PLUGIN_TYPE)
@Name("MockJoiner")
public class MockJoiner extends BatchJoiner<StructuredRecord, StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;
  private Map<String, Schema> inputSchemas;
  private Schema outputSchema;

  public MockJoiner(Config config) {
    this.config = config;
  }

  private void validateInputStages(Map<String, Schema> inputSchemas, List<String> inputStages) {
    if (!inputStages.containsAll(inputSchemas.keySet())) {
      throw new RuntimeException("inputStages: " + inputStages + "doesn't have all stages in inputSchemas: " +
                                  inputSchemas.keySet());
    }
  }

  @Override
  public void configurePipeline(MultiInputPipelineConfigurer pipelineConfigurer) {
    MultiInputStageConfigurer stageConfigurer = pipelineConfigurer.getMultiInputStageConfigurer();
    Map<String, Schema> inputSchemas = stageConfigurer.getInputSchemas();
    List<String> inputStages = stageConfigurer.getInputStages();
    config.validateConfig(inputSchemas, stageConfigurer.getFailureCollector());
    validateInputStages(inputSchemas, inputStages);
    stageConfigurer.setOutputSchema(getOutputSchema(inputSchemas));
  }

  @Override
  public void initialize(BatchJoinerRuntimeContext context) throws Exception {
    inputSchemas = context.getInputSchemas();
    outputSchema = context.getOutputSchema();
    List<String> inputStages = context.getInputStages();
    validateInputStages(inputSchemas, inputStages);
  }

  @Override
  public StructuredRecord joinOn(String stageName, StructuredRecord record) throws Exception {
    List<Schema.Field> fields = new ArrayList<>();
    Schema schema = record.getSchema();

    // TODO create output record based on fields properties
    Map<String, List<String>> stageToJoinKey = config.getJoinKeys();
    List<String> joinKeys = stageToJoinKey.get(stageName);
    int i = 1;
    for (String joinKey : joinKeys) {
      Schema.Field joinField = Schema.Field.of(String.valueOf(i++), schema.getField(joinKey).getSchema());
      fields.add(joinField);
    }
    Schema keySchema = Schema.recordOf("join.key", fields);
    StructuredRecord.Builder keyRecordBuilder = StructuredRecord.builder(keySchema);
    i = 1;
    for (String joinKey : joinKeys) {
      keyRecordBuilder.set(String.valueOf(i++), record.get(joinKey));
    }

    return keyRecordBuilder.build();
  }

  @Override
  public JoinConfig getJoinConfig() {
    return new JoinConfig(config.getRequiredInputs());
  }

  @Override
  public StructuredRecord merge(StructuredRecord joinKey, Iterable<JoinElement<StructuredRecord>> joinRow) {
    StructuredRecord.Builder outRecordBuilder;
    outRecordBuilder = StructuredRecord.builder(outputSchema);

    for (JoinElement<StructuredRecord> joinElement : joinRow) {
      StructuredRecord record = joinElement.getInputRecord();
      for (Schema.Field field : record.getSchema().getFields()) {
        outRecordBuilder.set(field.getName(), record.get(field.getName()));
      }
    }
    return outRecordBuilder.build();
  }

  private Schema getOutputSchema(Map<String, Schema> inputSchemas) {
    // sort the input schemas by input names to get the deterministic order of fields for output schema
    Map<String, Schema> sortedMap = new TreeMap<>(inputSchemas);
    List<Schema.Field> outputFields = new ArrayList<>();
    Iterable<String> requiredInputs = config.getRequiredInputs();

    // TODO use selectedFields
    for (Map.Entry<String, Schema> entry : sortedMap.entrySet()) {
      Schema inputSchema = entry.getValue();
      if (Iterables.contains(requiredInputs, entry.getKey())) {
        for (Schema.Field inputField : inputSchema.getFields()) {
          outputFields.add(Schema.Field.of(inputField.getName(), inputField.getSchema()));
        }
      } else { // mark fields as nullable
        for (Schema.Field inputField : inputSchema.getFields()) {
          outputFields.add(Schema.Field.of(inputField.getName(),
                                           Schema.nullableOf(inputField.getSchema())));
        }
      }
    }
    return Schema.recordOf("join.output", outputFields);
  }

  /**
   * Config for join plugin
   */
  public static class Config extends PluginConfig {
    private final String joinKeys;
    private final String selectedFields;
    @Nullable
    private final String requiredInputs;


    public Config() {
      this.joinKeys = "joinKeys";
      this.selectedFields = "selectedFields";
      this.requiredInputs = "requiredInputs";
    }

    private void validateConfig(Map<String, Schema> inputSchemas, FailureCollector collector) {
      if (joinKeys == null || joinKeys.isEmpty()) {
        collector.addFailure("Config property joinKeys is either null or empty",
                             "Provide non-empty joinKeys config property").withConfigProperty("joinKeys");
        collector.getOrThrowException();
      }

      List<String> multipleJoinKeys = Lists.newArrayList(Splitter.on('&').trimResults()
                                                           .omitEmptyStrings().split(joinKeys));
      for (String joinKey : multipleJoinKeys) {
        Map<String, String> map = new LinkedHashMap<>();
        List<String> perStageJoinKeys = Lists.newArrayList(Splitter.on('=').trimResults()
                                                             .omitEmptyStrings().split(joinKey));
        for (String perStageKey : perStageJoinKeys) {
          List<String> stageKey = Lists.newArrayList(Splitter.on('.').trimResults()
                                                       .omitEmptyStrings().split(perStageKey));
          if (stageKey.size() != 2) {
            collector.addFailure(String.format("Join key is not specified in stageName.columnName " +
                                                 "format for key %s", perStageKey),
                                 "Make sure syntax for joinKeys config property is correct")
              .withConfigProperty("joinKeys");
          } else {
            map.putIfAbsent(stageKey.get(0), stageKey.get(1));
          }
        }

        Schema prevSchema = null;
        String prevStage = null;
        for (Map.Entry<String, String> entry : map.entrySet()) {
          String stageName = entry.getKey();
          String keyField = entry.getValue();
          Schema.Field field = inputSchemas.get(stageName).getField(keyField);
          if (field == null) {
            collector.addFailure(String.format("Join key field %s is not present in input schema", field),
                                 "Make sure all the join keys are present in the input schema")
              .withConfigElement("joinKeys", joinKey);
            continue;
          }

          if (prevSchema != null && !prevSchema.equals(field.getSchema())) {
            collector.addFailure(String.format("Schema of joinKey field %s.%s does not match with other join keys.",
                                               stageName, keyField), "Make sure all the join keys are of same type")
              .withConfigElement("joinKeys", joinKey)
              .withInputSchemaField(keyField, stageName).withInputSchemaField(keyField, prevStage);
          }
          prevSchema = field.getSchema();
          prevStage = stageName;
        }
      }

      collector.getOrThrowException();
    }

    /**
     * Converts join keys to map of per stage join keys For example,
     * customers.id=items.cust_id&customers.name=items.cust_name
     * will get converted to customers -> (id,name) and items -> (cust_id,cust_name)
     * @return map of stage to join key fields from that stage
     */
    private Map<String, List<String>> getJoinKeys() {
      Map<String, List<String>> stageToKey = new HashMap<>();
      Iterable<String> multipleJoinKeys = Splitter.on('&').trimResults().omitEmptyStrings().split(joinKeys);
      for (String key : multipleJoinKeys) {
        Iterable<String> perStageJoinKeys = Splitter.on('=').trimResults().omitEmptyStrings().split(key);
        for (String perStageKey : perStageJoinKeys) {
          Iterable<String> stageKey = Splitter.on('.').trimResults().omitEmptyStrings().split(perStageKey);
          String stageName = Iterables.get(stageKey, 0);
          List<String> listOfKeys = stageToKey.get(stageName);
          if (listOfKeys == null) {
            listOfKeys = new ArrayList<>();
            stageToKey.put(stageName, listOfKeys);
          }
          listOfKeys.add(Iterables.get(stageKey, 1));
        }
      }

      return stageToKey;
    }

    private Iterable<String> getRequiredInputs() {
      return Splitter.on(',').trimResults().omitEmptyStrings().split(requiredInputs);
    }
  }

  public static ETLPlugin getPlugin(String joinKeys, String requiredInputs, String selectedFields) {
    Map<String, String> properties = new HashMap<>();
    properties.put("joinKeys", joinKeys);
    properties.put("requiredInputs", requiredInputs);
    properties.put("selectedFields", selectedFields);
    return new ETLPlugin("MockJoiner", BatchJoiner.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("joinKeys", new PluginPropertyField("joinKeys", "", "string", true, false));
    properties.put("requiredInputs", new PluginPropertyField("requiredInputs", "", "string", true, false));
    properties.put("selectedFields", new PluginPropertyField("selectedFields", "", "string", true, false));
    return PluginClass.builder().setName("MockJoiner").setType(BatchJoiner.PLUGIN_TYPE)
             .setDescription("").setClassName(MockJoiner.class.getName()).setProperties(properties)
             .setConfigFieldName("config").build();
  }
}
