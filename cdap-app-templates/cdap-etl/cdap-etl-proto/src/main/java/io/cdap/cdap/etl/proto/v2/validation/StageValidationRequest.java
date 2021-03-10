/*
 * Copyright © 2019 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.proto.v2.validation;

import io.cdap.cdap.etl.proto.v2.ETLStage;

import java.util.Collections;
import java.util.List;

/**
 * Request to validate a pipeline stage.
 */
public class StageValidationRequest {
  private final ETLStage stage;
  private final List<StageSchema> inputSchemas;
  private final Boolean resolveMacrosFromPreferences;

  public StageValidationRequest(ETLStage stage,
                                List<StageSchema> inputSchemas,
                                boolean resolveMacrosFromPreferences) {
    this.stage = stage;
    this.inputSchemas = inputSchemas;
    this.resolveMacrosFromPreferences = resolveMacrosFromPreferences;
  }

  public ETLStage getStage() {
    return stage;
  }

  public List<StageSchema> getInputSchemas() {
    return inputSchemas == null ? Collections.emptyList() : inputSchemas;
  }

  public boolean getResolveMacrosFromPreferences() {
    return resolveMacrosFromPreferences != null ? resolveMacrosFromPreferences : false;
  }

  /**
   * Validate that the request contains all required information. This should be called whenever this instance is
   * created by deserializing user provided input.
   *
   * @throws IllegalArgumentException if the request is invalid
   */
  public void validate() {
    if (stage == null) {
      throw new IllegalArgumentException("Pipeline stage config must be provided in the validation request.");
    }
    stage.validate();
    for (StageSchema inputSchema : getInputSchemas()) {
      inputSchema.validate();
    }
  }
}
