/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
package io.cdap.cdap.etl.common;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.MultiInputStageConfigurer;
import io.cdap.cdap.etl.api.MultiOutputStageConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * This stores the input schemas that is passed to this stage from other stages in the pipeline and
 * the output schema that could be sent to the next stages from this stage.
 * Currently we only allow multiple input/output schema per stage except for {@link io.cdap.cdap.etl.api.Joiner}
 * where we allow multiple input schemas
 */
public class DefaultStageConfigurer implements StageConfigurer, MultiInputStageConfigurer, MultiOutputStageConfigurer {
  private static final String STAGE = "stage";
  private Schema outputSchema;
  private Schema outputErrorSchema;
  private boolean errorSchemaSet;
  protected Map<String, Schema> inputSchemas;
  protected Map<String, Schema> outputPortSchemas;
  private final List<ValidationFailure> failures;
  private final String stageName;

  public DefaultStageConfigurer(String stageName) {
    this.inputSchemas = new HashMap<>();
    this.outputPortSchemas = new HashMap<>();
    this.errorSchemaSet = false;
    this.failures = new ArrayList<>();
    this.stageName = stageName;
  }

  @Nullable
  public Schema getOutputSchema() {
    return outputSchema;
  }

  public Map<String, Schema> getOutputPortSchemas() {
    return outputPortSchemas;
  }

  @Override
  @Nullable
  public Schema getInputSchema() {
    return inputSchemas.isEmpty() ? null : inputSchemas.entrySet().iterator().next().getValue();
  }

  @Override
  public void setOutputSchemas(Map<String, Schema> outputSchemas) {
    outputPortSchemas.putAll(outputSchemas);
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

  @Override
  public void setErrorSchema(@Nullable Schema errorSchema) {
    this.outputErrorSchema = errorSchema;
    errorSchemaSet = true;
  }

  @Override
  public ValidationFailure addFailure(String message, @Nullable String correctiveAction) {
    ValidationFailure failure = new ValidationFailure(message, correctiveAction);
    failures.add(failure);
    return failure;
  }

  @Override
  public void throwIfFailure() throws ValidationException {
    if (!failures.isEmpty()) {
      for (ValidationFailure failure : failures) {
        List<ValidationFailure.Cause> causes = failure.getCauses();
        if (causes.isEmpty()) {
          causes.add(new ValidationFailure.Cause().addAttribute(STAGE, stageName));
          continue;
        }
        for (ValidationFailure.Cause cause : causes) {
          // stage name is added by the configurer before throwing the validation exception
          cause.addAttribute(STAGE, stageName);
        }
      }

      // throw a validation exception if this configurer has any stage validation failures
      throw new ValidationException(failures);
    }
  }

  public List<ValidationFailure> getValidationFailures() {
    return failures;
  }

  public Schema getErrorSchema() {
    if (errorSchemaSet) {
      return outputErrorSchema;
    }
    // only joiners can have multiple input schemas, and joiners can't emit errors
    return inputSchemas.isEmpty() ? null : inputSchemas.values().iterator().next();
  }

  public void addInputSchema(String inputStageName, @Nullable Schema inputSchema) {
    inputSchemas.put(inputStageName, inputSchema);
  }
}

