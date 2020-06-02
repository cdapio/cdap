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

import javax.annotation.Nullable;

/**
 * Represents an input stage to the auto join.
 */
public class JoinStage {
  private final String stageName;
  private final Schema schema;
  private final boolean required;
  private final boolean broadcast;

  private JoinStage(String stageName, Schema schema, boolean required, boolean broadcast) {
    this.stageName = stageName;
    this.schema = schema;
    this.required = required;
    this.broadcast = broadcast;
  }

  public String getStageName() {
    return stageName;
  }

  @Nullable
  public Schema getSchema() {
    return schema;
  }

  public boolean isRequired() {
    return required;
  }

  public boolean isBroadcast() {
    return broadcast;
  }

  public static Builder builder(String stageName, @Nullable Schema schema) {
    return new Builder(stageName, schema);
  }

  public static Builder builder(JoinStage existing) {
    return new Builder(existing.stageName, existing.schema)
      .setBroadcast(existing.broadcast)
      .setRequired(existing.required);
  }

  /**
   * Builds a JoinStage.
   */
  public static class Builder {
    private final String stageName;
    private final Schema schema;
    private boolean required;
    private boolean broadcast;

    private Builder(String stageName, Schema schema) {
      this.stageName = stageName;
      this.schema = schema;
      this.required = true;
      this.broadcast = false;
    }

    public Builder isRequired() {
      return setRequired(true);
    }

    public Builder isOptional() {
      return setRequired(false);
    }

    /**
     * Set whether the stage is required. Joining two required stages is an inner join. Joining a required stage
     * with an optional stage is a left outer join. Joining two optional stages is an outer join.
     */
    public Builder setRequired(boolean required) {
      this.required = required;
      return this;
    }

    /**
     * Hint that the stage data should be broadcast during the join. In order to be broadcast, the stage data must
     * be below 8gb and fit entirely in memory. You cannot broadcast both sides of a join.
     * This is just a hint and will not always be honored.
     * MapReduce pipelines will currently ignore this flag. Spark pipelines will hint to Spark to broadcast, but Spark
     * may still decide to do a normal join depending on the type of join being performed and the datasets involved.
     */
    public Builder setBroadcast(boolean broadcast) {
      this.broadcast = broadcast;
      return this;
    }

    /**
     * @return a valid JoinStage
     */
    public JoinStage build() {
      return new JoinStage(stageName, schema, required, broadcast);
    }
  }
}
