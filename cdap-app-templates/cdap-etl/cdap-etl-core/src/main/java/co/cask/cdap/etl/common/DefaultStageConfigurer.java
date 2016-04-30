/**
 *
 */
/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.etl.common;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.StageConfigurer;

import java.util.Objects;
import javax.annotation.Nullable;


/**
 * This stores the input schema that is passed to this stage from other stages in the pipeline and
 * the output schema that could be sent to the next stages from this stage.
 * Currently we only allow a single input/output schema per stage.
 */
public class DefaultStageConfigurer implements StageConfigurer {
  private Schema outputSchema;
  private Schema inputSchema;
  private final String stageName;
  boolean inputSchemaSet;

  public DefaultStageConfigurer(String stageName) {
    this.stageName = stageName;
    this.inputSchemaSet = false;
  }

  @Nullable
  public Schema getOutputSchema() {
    return outputSchema;
  }

  @Override
  @Nullable
  public Schema getInputSchema() {
    return inputSchema;
  }

  @Override
  public void setOutputSchema(@Nullable Schema outputSchema) {
    this.outputSchema = outputSchema;
  }

  public void setInputSchema(@Nullable Schema inputSchema) {
    if (this.inputSchemaSet && !Objects.equals(this.inputSchema, inputSchema)) {
      throw new IllegalArgumentException(
        String.format("Two different input schema were set for stage %s. Schema1 = %s. Schema2 = %s.",
                      stageName, this.inputSchema, inputSchema));
    }
    this.inputSchema = inputSchema;
    this.inputSchemaSet = true;
  }
}

