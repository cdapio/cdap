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

package io.cdap.cdap.etl.spark.join;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.join.JoinField;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Request to join some collection to another collection.
 */
public class JoinRequest {
  private final String stageName;
  private final String leftStage;
  private final List<String> leftKey;
  private final Schema leftSchema;
  private final boolean leftRequired;
  private final boolean nullSafe;
  private final List<JoinField> fields;
  private final Schema outputSchema;
  private final List<JoinCollection> toJoin;
  private final Integer numPartitions;

  public JoinRequest(String stageName, String leftStage, List<String> leftKey, Schema leftSchema, boolean leftRequired,
                     boolean nullSafe, List<JoinField> fields, Schema outputSchema, List<JoinCollection> toJoin,
                     @Nullable Integer numPartitions) {
    this.stageName = stageName;
    this.leftStage = leftStage;
    this.leftKey = leftKey;
    this.leftRequired = leftRequired;
    this.nullSafe = nullSafe;
    this.fields = fields;
    this.leftSchema = leftSchema;
    this.outputSchema = outputSchema;
    this.toJoin = toJoin;
    this.numPartitions = numPartitions;
  }

  public String getStageName() {
    return stageName;
  }

  public String getLeftStage() {
    return leftStage;
  }

  public Schema getLeftSchema() {
    return leftSchema;
  }

  public List<String> getLeftKey() {
    return leftKey;
  }

  public boolean isLeftRequired() {
    return leftRequired;
  }

  public boolean isNullSafe() {
    return nullSafe;
  }

  public List<JoinField> getFields() {
    return fields;
  }

  public List<JoinCollection> getToJoin() {
    return toJoin;
  }

  public Schema getOutputSchema() {
    return outputSchema;
  }

  @Nullable
  public Integer getNumPartitions() {
    return numPartitions;
  }
}
