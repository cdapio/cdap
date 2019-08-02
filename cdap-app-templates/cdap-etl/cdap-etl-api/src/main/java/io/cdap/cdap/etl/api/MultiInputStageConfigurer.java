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

package io.cdap.cdap.etl.api;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * This stores the input schemas that are passed to this stage from other stages in the pipeline and
 * the output schema that could be sent to the next stages from this stage.
 */
@Beta
public interface MultiInputStageConfigurer {

  /**
   * Get the map of input stageName to input schema for this stage, or return empty map if its unknown.
   *
   * @return map of input schemas
   */
  Map<String, Schema> getInputSchemas();

  /**
   * Set output schema for this stage, or null if its unknown.
   *
   * @param outputSchema output schema for this stage
   */
  void setOutputSchema(@Nullable Schema outputSchema);

  /**
   * Add validation failure to this configurer.
   *
   * @param message failure message
   * @param correctiveAction corrective action
   * @return a validation failure
   */
  ValidationFailure addFailure(String message, @Nullable String correctiveAction);

  /**
   * Throws validation exception if there are any failures that are added to the configurer through
   * {@link #addFailure(String, String)}.
   *
   * @throws ValidationException if there are any validation failures
   */
  void throwIfFailure() throws ValidationException;
}
