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

import com.google.common.base.Strings;
import io.cdap.cdap.etl.proto.v2.ETLStage;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Request to validate a pipeline stage.
 */
public class StageValidationRequest {

  private final ETLStage stage;
  private final List<StageSchema> inputSchemas;
  private final Boolean resolveMacrosFromPreferences;
  private final String doNotSkipInvalidMacroForFunctions;

  public StageValidationRequest(ETLStage stage,
                                List<StageSchema> inputSchemas,
                                boolean resolveMacrosFromPreferences) {
    this(stage, inputSchemas, resolveMacrosFromPreferences, null);
  }
  public StageValidationRequest(ETLStage stage,
      List<StageSchema> inputSchemas,
      boolean resolveMacrosFromPreferences,
      String doNotSkipInvalidMacroForFunctions) {
    this.stage = stage;
    this.inputSchemas = inputSchemas;
    this.resolveMacrosFromPreferences = resolveMacrosFromPreferences;
    this.doNotSkipInvalidMacroForFunctions = doNotSkipInvalidMacroForFunctions;
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
   *  This method will return macro function names for which invalid macros should not be skipped.
   * @return Set of macro function names
   */
  public Set<String> getDoNotSkipInvalidMacroForFunctions() {
    Set<String> doNotSkipInvalidMacroForFunctionsSet = new HashSet<>();
    if (!Strings.isNullOrEmpty(doNotSkipInvalidMacroForFunctions)) {
      doNotSkipInvalidMacroForFunctionsSet.addAll(Arrays.stream(doNotSkipInvalidMacroForFunctions.split(","))
                                                    .map(String::trim)
                                                    .filter(s -> !s.isEmpty())
                                                    .collect(Collectors.toSet()));
    }
    return doNotSkipInvalidMacroForFunctionsSet;
  }

  /**
   * Validate that the request contains all required information. This should be called whenever
   * this instance is created by deserializing user provided input.
   *
   * @throws IllegalArgumentException if the request is invalid
   */
  public void validate() {
    if (stage == null) {
      throw new IllegalArgumentException(
          "Pipeline stage config must be provided in the validation request.");
    }
    stage.validate();
    for (StageSchema inputSchema : getInputSchemas()) {
      inputSchema.validate();
    }
  }
}
