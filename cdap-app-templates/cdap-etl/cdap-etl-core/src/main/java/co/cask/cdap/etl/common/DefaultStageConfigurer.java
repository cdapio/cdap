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
import co.cask.cdap.etl.api.MultiInputStageConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * This stores the input schemas that is passed to this stage from other stages in the pipeline and
 * the output schema that could be sent to the next stages from this stage.
 * Currently we only allow multiple input/output schema per stage except for {@link co.cask.cdap.etl.api.Joiner}
 * where we allow multiple input schemas
 */
public class DefaultStageConfigurer implements StageConfigurer, MultiInputStageConfigurer {
  private Schema outputSchema;
  private final String stageName;
  private Map<String, Schema> inputSchemas;

  public DefaultStageConfigurer(String stageName) {
    this.stageName = stageName;
    this.inputSchemas = new HashMap<>();
  }

  @Nullable
  public Schema getOutputSchema() {
    return outputSchema;
  }

  @Override
  @Nullable
  public Schema getInputSchema() {
    return inputSchemas.isEmpty() ? null : inputSchemas.entrySet().iterator().next().getValue();
  }

  @Nullable
  @Override
  public Map<String, Schema> getInputSchemas() {
    return inputSchemas;
  }

  @Override
  public void setOutputSchema(@Nullable Schema outputSchema) {
    this.outputSchema = outputSchema;
  }

  public void addInputSchema(String inputStageName, @Nullable Schema inputSchema) {
    inputSchemas.put(inputStageName, inputSchema);
  }
}

