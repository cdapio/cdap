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

package io.cdap.cdap.etl.common.plugin;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.JoinConfig;
import io.cdap.cdap.etl.api.JoinElement;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoinerContext;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinDistribution;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.cdap.etl.api.join.JoinStage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An implementation of {@link BatchJoiner} using a {@link BatchAutoJoiner}.
 *
 * @param <INPUT_RECORD> type of input record
 */
public class JoinerBridge<INPUT_RECORD> extends BatchJoiner<StructuredRecord, INPUT_RECORD, StructuredRecord> {
  private static final String SALT_COLUMN = "salt";
  private final BatchAutoJoiner autoJoiner;
  private final JoinDefinition joinDefinition;
  private final Set<String> requiredStages;
  private final Map<String, List<String>> joinKeys;
  private final Map<String, List<JoinField>> stageFields;
  private final Random saltGenerator;
  private Schema keySchema;
  private Schema outputSchema;

  public JoinerBridge(String stageName, BatchAutoJoiner autoJoiner, JoinDefinition joinDefinition) {
    // if this is not an inner join and the output schema is not set,
    // we have no way of determining what the output schema should be and need to error out.
    if (joinDefinition.getOutputSchema() == null &&
      joinDefinition.getStages().stream().anyMatch(s -> !s.isRequired())) {
      throw new IllegalArgumentException(
        String.format("An output schema could not be generated for joiner stage '%s'. " +
                        "Provide the expected output schema directly.", stageName));
    }
    this.autoJoiner = autoJoiner;
    this.joinDefinition = joinDefinition;
    this.requiredStages = joinDefinition.getStages().stream()
      .filter(JoinStage::isRequired)
      .map(JoinStage::getStageName)
      .collect(Collectors.toSet());
    JoinCondition condition = joinDefinition.getCondition();
    if (condition.getOp() != JoinCondition.Op.KEY_EQUALITY) {
      // will never happen unless we add more join conditions, at which point this needs to be updated.
      throw new IllegalStateException("Unsupported join condition operation " + condition.getOp());
    }
    JoinCondition.OnKeys onKeys = (JoinCondition.OnKeys) condition;
    this.joinKeys = onKeys.getKeys().stream()
      .collect(Collectors.toMap(JoinKey::getStageName, JoinKey::getFields));
    this.stageFields = new HashMap<>();
    for (JoinField field : joinDefinition.getSelectedFields()) {
      List<JoinField> fields = stageFields.computeIfAbsent(field.getStageName(), k -> new ArrayList<>());
      fields.add(field);
    }
    saltGenerator = new Random();
  }

  @Override
  public void prepareRun(BatchJoinerContext context) throws Exception {
    autoJoiner.prepareRun(context);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchJoinerContext context) {
    autoJoiner.onRunFinish(succeeded, context);
  }

  @Override
  public JoinConfig getJoinConfig() {
    return new JoinConfig(requiredStages);
  }

  @Override
  @Deprecated
  public StructuredRecord joinOn(String stageName, INPUT_RECORD input) {
    if (!(input instanceof StructuredRecord)) {
      // don't expect this to ever be the case since all existing plugins use StructuredRecord,
      // but it is technically possible.
      throw new IllegalArgumentException(String.format(
        "Received an input record of unsupported type '%s' from stage '%s'.",
        input.getClass().getName(), stageName));
    }

    List<String> key = joinKeys.get(stageName);
    if (key == null) {
      // this should not happen, it should be caught by the pipeline app at configure or prepare time and failed then
      throw new IllegalArgumentException(
        String.format("Received data from stage '%s', but the stage was not included as part of the join. " +
                        "Check the plugin to make sure it is including all input stages.", stageName));
    }

    StructuredRecord inputRecord = (StructuredRecord) input;
    if (keySchema == null) {
      keySchema = getKeySchema(stageName, inputRecord.getSchema(), key);
    }

    StructuredRecord.Builder keyRecord = getKeyRecordBuilder(key, inputRecord);
    return keyRecord.build();
  }

  @Override
  public Collection<StructuredRecord> getJoinKeys(String stageName, INPUT_RECORD record) throws Exception {
    if (!(record instanceof StructuredRecord)) {
      // don't expect this to ever be the case since all existing plugins use StructuredRecord,
      // but it is technically possible.
      throw new IllegalArgumentException(String.format(
        "Received an input record of unsupported type '%s' from stage '%s'.",
        record.getClass().getName(), stageName));
    }

    List<String> key = joinKeys.get(stageName);
    if (key == null) {
      // this should not happen, it should be caught by the pipeline app at configure or prepare time and failed then
      throw new IllegalArgumentException(
        String.format("Received data from stage '%s', but the stage was not included as part of the join. " +
                        "Check the plugin to make sure it is including all input stages.", stageName));
    }

    StructuredRecord inputRecord = (StructuredRecord) record;
    if (keySchema == null) {
      keySchema = getKeySchema(stageName, inputRecord.getSchema(), key);
    }

    JoinDistribution distribution = joinDefinition.getDistribution();
    List<StructuredRecord> keyRecords = new ArrayList<>();

    StructuredRecord.Builder keyRecord = getKeyRecordBuilder(key, inputRecord);

    // If distribution is not enabled then return the record without any changes
    if (distribution == null) {
      keyRecords.add(keyRecord.build());
      return keyRecords;
    }

    int distributionFactor = distribution.getDistributionFactor();

    // If this is the skewed stage then we need to add salt
    if (stageName.equals(distribution.getSkewedStageName())) {
      keyRecord.set(SALT_COLUMN, saltGenerator.nextInt(distributionFactor));
      keyRecords.add(keyRecord.build());
      return keyRecords;
    }

    // This is not the skewed stage so we need to explode it
    for (int i = 0; i < distributionFactor; i++) {
      StructuredRecord.Builder recordBuilder = getKeyRecordBuilder(key, inputRecord);
      recordBuilder.set(SALT_COLUMN, i);
      keyRecords.add(recordBuilder.build());
    }
    return keyRecords;
  }

  private StructuredRecord.Builder getKeyRecordBuilder(List<String> key, StructuredRecord inputRecord) {
    StructuredRecord.Builder keyRecord = StructuredRecord.builder(keySchema);
    int fieldNum = 0;
    for (String keyField : key) {
      String translatedName = "f" + fieldNum++;
      keyRecord.set(translatedName, inputRecord.get(keyField));
    }
    return keyRecord;
  }

  // JoinDefinition can have something like A.x = B.y and A.z = B.w
  // However, the keys emitted for both A and B must be exactly the same to make sure they
  // all get grouped together. If the key for A has fields (x,z) while the key for B has fields (y,w),
  // they will not match. To ensure they do match, we generate field names f0, f1, f2, etc.
  // Also, it is valid to have a condition like A.x = B.y where the schemas for A.x and B.y are not exactly
  // the same. For example, A.x may be an integer, while B.y is a nullable integer.
  // or A.x could be a long while B.y is a timestamp (whose physical type is a long).
  // To ensure that the schema is the same, we always use a schema of the nullable type.
  private Schema getKeySchema(String stageName, Schema schema, List<String> key) {
    List<Schema.Field> fields = new ArrayList<>(key.size());
    int fieldNum = 0;
    for (String keyField : key) {
      Schema fieldSchema = schema.getField(keyField).getSchema();
      if (fieldSchema == null) {
        // should never happen, this should be checked during validation.
        throw new IllegalStateException(
          String.format("Key field '%s' does not exist from stage '%s'", keyField, stageName));
      }
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
      Schema keyFieldSchema = Schema.nullableOf(Schema.of(fieldSchema.getType()));
      String translatedName = "f" + fieldNum++;
      fields.add(Schema.Field.of(translatedName, keyFieldSchema));
    }
    if (joinDefinition.getDistribution() != null) {
      fields.add(Schema.Field.of(SALT_COLUMN, Schema.of(Schema.Type.INT)));
    }
    return Schema.recordOf("key", fields);
  }

  @Override
  public StructuredRecord merge(StructuredRecord structuredRecord,
                                Iterable<JoinElement<INPUT_RECORD>> joinResult) {
    if (outputSchema == null) {
      outputSchema = joinDefinition.getOutputSchema();
      // can only still be null if this is an inner join, which means we can generate the output schema
      // based on the JoinElements.
      if (outputSchema == null) {
        outputSchema = generateOutputSchema(joinResult);
      }
    }

    StructuredRecord.Builder joined = StructuredRecord.builder(outputSchema);
    for (JoinElement<INPUT_RECORD> joinElement : joinResult) {
      String stageName = joinElement.getStageName();
      StructuredRecord record = (StructuredRecord) joinElement.getInputRecord();

      List<JoinField> outputFields = stageFields.get(stageName);
      for (JoinField outputField : outputFields) {
        String originalName = outputField.getFieldName();
        String outputFieldName = outputField.getAlias() == null ? originalName : outputField.getAlias();
        joined.set(outputFieldName, record.get(originalName));
      }
    }

    return joined.build();
  }

  private Schema generateOutputSchema(Iterable<JoinElement<INPUT_RECORD>> elements) {
    Map<String, Schema> stageSchemas = new HashMap<>();
    for (JoinElement<INPUT_RECORD> joinElement : elements) {
      StructuredRecord joinRecord = (StructuredRecord) joinElement.getInputRecord();
      stageSchemas.put(joinElement.getStageName(), joinRecord.getSchema());
    }

    List<Schema.Field> fields = new ArrayList<>(joinDefinition.getSelectedFields().size());
    for (JoinField joinField : joinDefinition.getSelectedFields()) {
      String originalName = joinField.getFieldName();
      String outputName = joinField.getAlias() == null ? originalName : joinField.getAlias();
      Schema stageSchema = stageSchemas.get(joinField.getStageName());
      if (stageSchema == null) {
        // should not be possible, should be validated earlier
        throw new IllegalArgumentException(String.format(
          "Unable to select field '%s' from stage '%s' because data for the stage could not be found.",
          originalName, joinField.getStageName()));
      }
      Schema.Field stageField = stageSchema.getField(originalName);
      if (stageField == null) {
        // should not be possible, should be validated earlier
        throw new IllegalArgumentException(String.format(
          "Unable to select field '%s' from stage '%s' because the field for the stage could not be found.",
          originalName, joinField.getStageName()));
      }
      fields.add(Schema.Field.of(outputName, stageField.getSchema()));
    }
    return Schema.recordOf("joined", fields);
  }
}
