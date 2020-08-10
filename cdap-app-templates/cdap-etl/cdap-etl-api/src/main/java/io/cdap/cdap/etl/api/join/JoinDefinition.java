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

package io.cdap.cdap.etl.api.join;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.join.error.JoinError;
import io.cdap.cdap.etl.api.join.error.OutputSchemaError;
import io.cdap.cdap.etl.api.join.error.SelectedFieldError;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Specifies how a join should be executed.
 */
@Beta
public class JoinDefinition {
  private final List<JoinField> selectedFields;
  private final List<JoinStage> stages;
  private final JoinCondition condition;
  private final Schema outputSchema;
  private final JoinDistribution distribution;

  private JoinDefinition(List<JoinField> selectedFields, List<JoinStage> stages,
                         JoinCondition condition, Schema outputSchema, JoinDistribution distribution) {
    this.stages = Collections.unmodifiableList(stages);
    this.selectedFields = Collections.unmodifiableList(new ArrayList<>(selectedFields));
    this.condition = condition;
    this.outputSchema = outputSchema;
    this.distribution = distribution;
  }

  public JoinDistribution getDistribution() {
    return distribution;
  }

  public List<JoinField> getSelectedFields() {
    return selectedFields;
  }

  public List<JoinStage> getStages() {
    return stages;
  }

  public JoinCondition getCondition() {
    return condition;
  }

  public Schema getOutputSchema() {
    return outputSchema;
  }

  /**
   * @return a Builder to create a JoinSpecification.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds a JoinSpecification.
   */
  public static class Builder {
    private final List<JoinStage> stages;
    private final List<JoinField> selectedFields;
    private JoinCondition condition;
    private String schemaName;
    private Schema outputSchema;
    private JoinDistribution distribution;

    private Builder() {
      stages = new ArrayList<>();
      selectedFields = new ArrayList<>();
      schemaName = null;
      condition = null;
      distribution = null;
    }

    public Builder select(List<JoinField> selectedFields) {
      this.selectedFields.clear();
      this.selectedFields.addAll(selectedFields);
      return this;
    }

    public Builder select(JoinField... fields) {
      return select(Arrays.asList(fields));
    }

    public Builder from(Collection<JoinStage> stages) {
      this.stages.clear();
      this.stages.addAll(stages);
      return this;
    }

    public Builder from(JoinStage... stages) {
      return from(Arrays.asList(stages));
    }

    public Builder on(JoinCondition condition) {
      this.condition = condition;
      return this;
    }

    public Builder setOutputSchemaName(@Nullable String name) {
      schemaName = name;
      return this;
    }

    /**
     * Set the distribution factor and stage name of the skewed stage. This should be set if the join being performed
     * is skewed (ie. joining a large dataset with a small dataset) and the "small" dataset is too large to broadcast
     * . The specified skewed stage must be marked as "required" and neither stage should be broadcast for
     * distribution to work.
     *
     * @param size      The number of distributions to split each key into. Note that the smaller dataset will grow by
     *                  this factor, values greater than 20 are not recommended
     * @param stageName The name of the input stage that contains the skewed data, this stage must be marked as
     *                  "required"
     * @return
     */
    public Builder setDistributionFactor(int size, String stageName) {
      this.distribution = new JoinDistribution(size, stageName);
      return this;
    }

    /**
     * Set the output schema for the join. This should only be set if the input JoinStages do not contain known
     * schemas. The most common scenario here is when the input schemas are not known when the pipeline is deployed
     * due to macros.
     * <p>
     * When all input schemas are known, if the expected output schema differs from this schema, an error
     * will be thrown.
     */
    public Builder setOutputSchema(Schema outputSchema) {
      this.outputSchema = outputSchema;
      return this;
    }

    /**
     * @return a valid JoinDefinition
     * @throws InvalidJoinException if the join is invalid
     */
    public JoinDefinition build() {
      if (stages.size() < 2) {
        throw new InvalidJoinException("At least two stages must be specified.");
      }

      List<JoinError> errors = new ArrayList<>();
      if (selectedFields.isEmpty()) {
        errors.add(new JoinError("At least one field must be selected."));
      }

      // validate the join condition
      if (condition == null) {
        errors.add(new JoinError("A join condition must be specified."));
      } else {
        errors.addAll(condition.validate(stages));
      }

      Schema generatedOutputSchema = getOutputSchema(errors);
      if (generatedOutputSchema != null && outputSchema != null) {
        // verify that the plugin defined output schema is compatible with the actual output schema
        validateSchemaCompatibility(generatedOutputSchema, outputSchema, errors);
      }

      if (distribution != null) {
        errors.addAll(distribution.validate(stages));
      }

      if (!errors.isEmpty()) {
        throw new InvalidJoinException(errors);
      }

      return new JoinDefinition(selectedFields, stages, condition,
        outputSchema == null ? generatedOutputSchema : outputSchema, distribution);
    }

    @Nullable
    private Schema getOutputSchema(List<JoinError> errors) {
      Set<String> outputFieldNames = new HashSet<>();
      List<Schema.Field> outputFields = new ArrayList<>(selectedFields.size());
      Map<String, JoinStage> stageMap = stages.stream()
        .collect(Collectors.toMap(JoinStage::getStageName, s -> s));

      for (JoinField field : selectedFields) {
        JoinStage joinStage = stageMap.get(field.getStageName());
        if (joinStage == null) {
          errors.add(new SelectedFieldError(field, String.format(
            "Selected field '%s'.'%s' is invalid because stage '%s' is not part of the join.",
            field.getStageName(), field.getFieldName(), field.getStageName())));
          continue;
        }
        Schema stageSchema = joinStage.getSchema();
        // schema is null if the schema is unknown
        // for example, when the pipeline is being deployed, the schema might not yet be known due to
        // macros not being evaluated yet.
        if (stageSchema == null) {
          return null;
        }
        Schema.Field schemaField = stageSchema.getField(field.getFieldName());
        if (schemaField == null) {
          errors.add(new SelectedFieldError(field, String.format(
            "Selected field '%s'.'%s' is invalid because stage '%s' does not contain field '%s'.",
            field.getStageName(), field.getFieldName(), field.getStageName(), field.getFieldName())));
          continue;
        }

        String outputFieldName = field.getAlias() == null ? field.getFieldName() : field.getAlias();
        if (!outputFieldNames.add(outputFieldName)) {
          errors.add(new SelectedFieldError(field, String.format(
            "Field '%s' from stage '%s' is a duplicate. Set an alias to make it unique.",
            outputFieldName, field.getStageName())));
          continue;
        }

        Schema outputFieldSchema = schemaField.getSchema();
        if (!joinStage.isRequired() && !outputFieldSchema.isNullable()) {
          outputFieldSchema = Schema.nullableOf(outputFieldSchema);
        }
        outputFields.add(Schema.Field.of(outputFieldName, outputFieldSchema));
      }

      if (!errors.isEmpty()) {
        return null;
      }

      return Schema.recordOf(schemaName == null ? "joined" : schemaName, outputFields);
    }

    /**
     * Validate that the first schema is compatible with the second. Schema s1 is compatible with schema s2 if all
     * the fields in s1 are present in s2 with the same type, or with the nullable version of the type. In addition,
     * s2 cannot contain any fields that are not present in s1.
     *
     * @param expected the expected schema
     * @param provided the provided schema
     */
    private static void validateSchemaCompatibility(Schema expected, Schema provided, List<JoinError> errors) {
      Set<String> missingFields = new HashSet<>();
      for (Schema.Field expectedField : expected.getFields()) {
        Schema.Field providedField = provided.getField(expectedField.getName());
        if (providedField == null) {
          missingFields.add(expectedField.getName());
          continue;
        }
        Schema expectedFieldSchema = expectedField.getSchema();
        Schema providedFieldSchema = providedField.getSchema();
        boolean expectedIsNullable = expectedFieldSchema.isNullable();
        boolean providedIsNullable = providedFieldSchema.isNullable();
        expectedFieldSchema = expectedIsNullable ? expectedFieldSchema.getNonNullable() : expectedFieldSchema;
        providedFieldSchema = providedIsNullable ? providedFieldSchema.getNonNullable() : providedFieldSchema;
        if (expectedFieldSchema.getType() != providedFieldSchema.getType()) {
          errors.add(new OutputSchemaError(
            expectedField.getName(), expectedFieldSchema.getDisplayName(),
            String.format("Provided schema does not match expected schema. " +
                "Field '%s' is a '%s' but is expected to be a '%s'",
              expectedField.getName(), expectedFieldSchema.getDisplayName(),
              providedFieldSchema.getDisplayName())));
          continue;
        }
        if (expectedIsNullable && !providedIsNullable) {
          errors.add(new OutputSchemaError(
            expectedField.getName(), expectedFieldSchema.getDisplayName(),
            String.format("Provided schema does not match expected schema. Field '%s' should be nullable",
              expectedField.getName())));
        }
      }

      if (!missingFields.isEmpty()) {
        errors.add(new JoinError(String.format(
          "Provided schema is missing fields: %s. Select fewer fields or update the output schema.",
          String.join(", ", missingFields))));
      }

      for (Schema.Field providedField : provided.getFields()) {
        String fieldName = providedField.getName();
        if (expected.getField(fieldName) == null) {
          errors.add(new OutputSchemaError(
            fieldName, null,
            String.format("The output schema contains extra field '%s' that not in the list of selected fields.",
              fieldName),
            "Select it from one of the inputs, or remove it from the output schema."));
        }
      }
    }
  }
}
