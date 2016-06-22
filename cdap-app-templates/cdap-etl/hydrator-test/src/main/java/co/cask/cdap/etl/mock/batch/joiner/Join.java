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

package co.cask.cdap.etl.mock.batch.joiner;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.JoinConfig;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.JoinResult;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Join plugin to perform joins on structured records
 */
@Plugin(type = BatchJoiner.PLUGIN_TYPE)
@Name("Join")
public class Join extends BatchJoiner<StructuredRecord, StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;
  private Schema outputSchema;

  public Join(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Map<String, Schema> inputSchemas = stageConfigurer.getInputSchemas();
    stageConfigurer.setOutputSchema(getOutputSchema(inputSchemas));
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
      Schema.Field joinField = Schema.Field.of("_" + i++, schema.getField(joinKey).getSchema());
      fields.add(joinField);
    }
    Schema keySchema = Schema.recordOf("join.key", fields);
    StructuredRecord.Builder keyRecordBuilder = StructuredRecord.builder(keySchema);
    i = 1;
    for (String joinKey : joinKeys) {
      keyRecordBuilder.set("_" + i++, record.get(joinKey));
    }

    return keyRecordBuilder.build();
  }

  @Override
  public JoinConfig getJoinConfig() {
    return new JoinConfig(config.joinType, config.getNumOfInputs());
  }

  @Override
  public void merge(StructuredRecord joinKey, List<JoinResult> joinResults, Emitter<StructuredRecord> emitter) {
    Set<Schema.Field> fields = new HashSet<>();
    StructuredRecord.Builder outRecordBuilder;

    for (JoinResult joinResult : joinResults) {
      if (outputSchema == null) {
        outputSchema = getOutputSchema(fields, joinResult);
      }
      outRecordBuilder = StructuredRecord.builder(outputSchema);
      for (JoinElement joinElement : joinResult.getJoinResult()) {
        StructuredRecord record = (StructuredRecord) joinElement.getJoinValue();
        for (Schema.Field field : record.getSchema().getFields()) {
          outRecordBuilder.set(field.getName(), record.get(field.getName()));
        }
      }
      emitter.emit(outRecordBuilder.build());
    }
  }

  private Schema getOutputSchema(Set<Schema.Field> fields, JoinResult joinResult) {
    for (JoinElement joinElement : joinResult.getJoinResult()) {
      StructuredRecord record = (StructuredRecord) joinElement.getJoinValue();
      for (Schema.Field field : record.getSchema().getFields()) {
        fields.add(field);
      }
    }
    return Schema.recordOf("join.output", fields);
  }

  private Schema getOutputSchema(Map<String, Schema> inputSchemas) {
    Set<Schema.Field> fields = new HashSet<>();

    // TODO use fields properties to create output schema
      for (Schema inputSchema: inputSchemas.values()) {
        List<Schema.Field> inputFields = inputSchema.getFields();
        for (Schema.Field inputField: inputFields) {
          fields.add(Schema.Field.of(inputField.getName(), inputField.getSchema()));
        }
      }
    return Schema.recordOf("join.output", fields);
  }

  /**
   * Config for join plugin
   */
  public static class Config extends PluginConfig {
    private final String joinKeys;
    private final String joinType;
    private final String numOfInputs;
    @Nullable
    private final String fieldsToSelect;
    @Nullable
    private final String fieldsToRename;
    @Nullable
    private final String requiredInputs;


    public Config() {
      this.joinKeys = "joinKeys";
      this.joinType = "joinType";
      this.numOfInputs = "numOfInputs";
      this.fieldsToSelect = "fieldsToSelect";
      this.fieldsToRename = "fieldsToRename";
      this.requiredInputs = "requiredInputs";
    }

    private void validateConfig() {
      validateNumOfInputs();

      if (joinKeys == null || joinKeys.isEmpty()) {
        throw new IllegalArgumentException(String.format(
          "join keys can not be empty or null for plugin %s", PLUGIN_CLASS));
      }

      Iterable<String> multipleJoinKeys = Splitter.on(':').trimResults().split(joinKeys);
      if (Iterables.size(multipleJoinKeys) == 0) {
        throw new IllegalArgumentException(String.format("There should be atleast %s join keys", numOfInputs));
      }

      for (String key : multipleJoinKeys) {
        int numOfStages = 0;
        Iterable<String> perStageJoinKeys = Splitter.on(',').trimResults().split(key);
        if (Iterables.size(perStageJoinKeys) != getNumOfInputs()) {
          throw new IllegalArgumentException(String.format("Number of join keys should be equal for all the stages " +
                                                             "for plugin %s", PLUGIN_TYPE));
        }

        for (String perStageKey : perStageJoinKeys) {
          Iterable<String> stageKey = Splitter.on('.').trimResults().split(perStageKey);
          if (Iterables.size(stageKey) != 2) {
            throw new IllegalArgumentException(String.format("Join key should be specified in stageName:joinKey format"
                                                               + " for %s of %s", perStageKey, PLUGIN_TYPE));
          }
          numOfStages++;
          if (numOfStages != getNumOfInputs()) {
            throw new IllegalArgumentException(String.format("There should be same number "
                                                               + " for %s of %s", perStageKey, PLUGIN_TYPE));
          }
        }
      }
    }

    private void validateNumOfInputs() {
      try {
        Integer.parseInt(numOfInputs);
      } catch (NumberFormatException exception) {
        throw new IllegalArgumentException(String.format("Number of inputs to %s should be integer but found %s ",
                                                         PLUGIN_TYPE, numOfInputs));
      }
    }

    private Map<String, List<String>> getJoinKeys() {
      Map<String, List<String>> stageToKey = new HashMap<>();
      Iterable<String> multipleJoinKeys = Splitter.on(':').trimResults().split(joinKeys);
      for (String key : multipleJoinKeys) {
        Iterable<String> perStageJoinKeys = Splitter.on(',').trimResults().split(key);
        for (String perStageKey : perStageJoinKeys) {
          Iterable<String> stageKey = Splitter.on('.').trimResults().split(perStageKey);
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

    private int getNumOfInputs() {
      return Integer.parseInt(numOfInputs);
    }
  }

  public static ETLPlugin getPlugin(String joinKeys, String joinType, String numOfInputs, String fieldsToSelect,
                                    String fieldsToRename, String nullableInputs) {
    Map<String, String> properties = new HashMap<>();
    properties.put("joinKeys", joinKeys);
    properties.put("joinType", joinType);
    properties.put("numOfInputs", numOfInputs);
    properties.put("fieldsToSelect", fieldsToSelect);
    properties.put("fieldsToRename", fieldsToRename);
    properties.put("requiredInputs", nullableInputs);
    return new ETLPlugin("Join", BatchJoiner.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("joinKeys", new PluginPropertyField("joinKeys", "", "string", true));
    properties.put("joinType", new PluginPropertyField("joinType", "", "string", true));
    properties.put("numOfInputs", new PluginPropertyField("numOfInputs", "", "string", true));
    properties.put("fieldsToSelect", new PluginPropertyField("fieldsToSelect", "", "string", true));
    properties.put("fieldsToRename", new PluginPropertyField("fieldsToRename", "", "string", true));
    properties.put("requiredInputs", new PluginPropertyField("requiredInputs", "", "string", true));
    return new PluginClass(BatchJoiner.PLUGIN_TYPE, "Join", "", Join.class.getName(),
                           "config", properties);
  }
}
