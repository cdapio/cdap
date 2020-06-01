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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.join.error.JoinError;
import io.cdap.cdap.etl.api.join.error.JoinKeyError;
import io.cdap.cdap.etl.api.join.error.JoinKeyFieldError;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A condition to join on.
 *
 * Currently joins can only be performed on equality on a set of fields from each stage.
 */
public class JoinCondition {
  private final Op op;

  private JoinCondition(Op op) {
    this.op = op;
  }

  public Op getOp() {
    return op;
  }

  /**
   * Validate that is condition is valid to use when joining the given stages.
   * Returns errors found when performing validation.
   *
   * @param joinStages the stages that will be joined on this condition
   * @return collection of errors found when performing validation.
   *   The collection will be empty if there are no problems.
   */
  public Collection<JoinError> validate(List<JoinStage> joinStages) {
    // no-op
    return Collections.emptyList();
  }

  /**
   * Condition operation.
   */
  public enum Op {
    KEY_EQUALITY
  }

  public static OnKeys.Builder onKeys() {
    return new OnKeys.Builder();
  }

  /**
   * Join on multiple keys from each stage.
   */
  public static class OnKeys extends JoinCondition {
    private final Set<JoinKey> keys;
    private final boolean nullSafe;

    private OnKeys(Set<JoinKey> keys, boolean nullSafe) {
      super(Op.KEY_EQUALITY);
      this.keys = Collections.unmodifiableSet(new HashSet<>(keys));
      this.nullSafe = nullSafe;
    }

    public Set<JoinKey> getKeys() {
      return keys;
    }

    public boolean isNullSafe() {
      return nullSafe;
    }

    @Override
    public Collection<JoinError> validate(List<JoinStage> joinStages) {
      List<JoinError> errors = new ArrayList<>();

      Map<String, JoinStage> stageMap = joinStages.stream()
        .collect(Collectors.toMap(JoinStage::getStageName, s -> s));

      for (JoinKey joinKey : keys) {
        String joinStageName = joinKey.getStageName();
        JoinStage joinStage = stageMap.get(joinStageName);
        // check that the stage for each key is in the list of stages
        if (joinStage == null) {
          errors.add(new JoinKeyError(joinKey,
                                      String.format("Join key for stage '%s' is invalid. " +
                                                      "Stage '%s' is not an input.", joinStageName, joinStageName)));
          continue;
        }
        // this happens if the schema for that stage is unknown.
        // for example, because of macros could not be evaluated yet.
        if (joinStage.getSchema() == null) {
          continue;
        }
        Set<String> fields = joinStage.getSchema().getFields().stream()
          .map(Schema.Field::getName)
          .collect(Collectors.toSet());

        // check that the key fields for each key is in fields for that stage
        // for example, when joining on A.id = B.uid, check that 'id' is in the fields for stage A and 'uid' for stage B
        Set<String> keysCopy = new HashSet<>(joinKey.getFields());
        keysCopy.removeAll(fields);
        if (keysCopy.size() == 1) {
          errors.add(new JoinKeyError(joinKey,
                                      String.format("Join key for stage '%s' is invalid. " +
                                                      "Field '%s' does not exist in the stage.",
                                                    joinStageName, keysCopy.iterator().next())));
        }
        if (keysCopy.size() > 1) {
          errors.add(new JoinKeyError(joinKey,
                                      String.format("Join key for stage '%s' is invalid. " +
                                                      "Fields %s do not exist in the stage.",
                                                    joinStageName, String.join(", ", keysCopy))));
        }
      }

      // end validation early since the rest of the validation assumes all stages and fields exist
      if (!errors.isEmpty()) {
        return errors;
      }

      // check that the keys have the same type.
      Iterator<JoinKey> keyIter = keys.iterator();
      JoinKey key1 = keyIter.next();
      // stage is guaranteed to be exist because of above validation
      Schema schema1 = stageMap.get(key1.getStageName()).getSchema();
      if (schema1 == null) {
        return errors;
      }
      while (keyIter.hasNext()) {
        JoinKey otherKey = keyIter.next();
        Schema otherSchema = stageMap.get(otherKey.getStageName()).getSchema();
        if (otherSchema == null) {
          continue;
        }
        Iterator<String> fieldIter = otherKey.getFields().iterator();
        for (String key1Field : key1.getFields()) {
          Schema key1Schema = getNonNullableFieldSchema(schema1, key1Field);

          // don't need to check hasNext() because already verified they are all the same length
          // in the build() method
          String otherField = fieldIter.next();
          Schema otherFieldSchema = getNonNullableFieldSchema(otherSchema, otherField);

          if (key1Schema.getType() != otherFieldSchema.getType()) {
            errors.add(new JoinKeyFieldError(otherKey.getStageName(), otherField, String.format(
              "Type mismatch on join keys. '%s'.'%s' is of type '%s' while '%s'.'%s' is of type '%s'.",
              key1.getStageName(), key1Field, key1Schema.getDisplayName(),
              otherKey.getStageName(), otherField, otherFieldSchema.getDisplayName())));
          }
        }
      }

      return errors;
    }

    private Schema getNonNullableFieldSchema(Schema schema, String field) {
      Schema fieldSchema = schema.getField(field).getSchema();
      return fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    }

    /**
     * Builds an OnKeys condition.
     */
    public static class Builder {
      private final Set<JoinKey> keys;
      private boolean nullSafe;

      private Builder() {
        this.keys = new HashSet<>();
        this.nullSafe = true;
      }

      public Builder addKey(JoinKey key) {
        this.keys.add(key);
        return this;
      }

      public Builder setKeys(Collection<JoinKey> keys) {
        this.keys.clear();
        this.keys.addAll(keys);
        return this;
      }

      /**
       * Whether to perform null safe equality on the join keys. Null safe means a null value equals another null value.
       * For example, when joining on A.id = B.id, if there are rows in both A and B with null ids, they will be
       * joined together. When not performing a null-safe join, rows with null ids would not get joined.
       *
       * Note that the behavior of traditional SQL systems is *not* to perform null safe joins. T
       */
      public Builder setNullSafe(boolean nullSafe) {
        this.nullSafe = nullSafe;
        return this;
      }

      /**
       * @return a join condition.
       * @throws InvalidJoinException if the join condition is invalid
       */
      public OnKeys build() {
        if (keys.size() < 2) {
          throw new InvalidJoinException("Must specify a join key for each input stage.");
        }
        Map<Integer, Set<String>> numFieldsToStages = new HashMap<>();
        for (JoinKey joinKey : keys) {
          int numFields = joinKey.getFields().size();
          Set<String> stages = numFieldsToStages.computeIfAbsent(numFields, k -> new HashSet<>());
          stages.add(joinKey.getStageName());
        }
        // this means there are stages with different number of fields.
        // it's the equivalent of trying to join on A.id = B.id and A.name = [null]
        if (numFieldsToStages.size() > 1) {
          StringBuilder message = new StringBuilder("Must join on the same number of fields for each stage. ");
          for (Map.Entry<Integer, Set<String>> entry : numFieldsToStages.entrySet()) {
            int numFields = entry.getKey();
            String fieldsStr = String.format("%d join field%s", numFields, numFields == 1 ? "" : "s");
            Set<String> stages = entry.getValue();
            if (stages.size() == 1) {
              message.append(String.format("Stage '%s' has %s. ", stages.iterator().next(), fieldsStr));
            } else {
              message.append(String.format("Stages %s have %s. ", String.join(", ", stages), fieldsStr));
            }
          }
          throw new InvalidJoinException(message.toString());
        }
        return new OnKeys(keys, nullSafe);
      }
    }
  }
}
