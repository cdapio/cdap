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
 */

package io.cdap.cdap.etl.validation;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default failure collector.
 */
public class DefaultFailureCollector implements FailureCollector {
  private static final String STAGE = "stage";
  private final String stageName;
  private final Map<String, Schema> inputSchemas;
  private final List<ValidationFailure> failures;

  /**
   * Default failure collector.
   *
   * @param stageName stage name
   * @param inputSchemas input schemas
   */
  public DefaultFailureCollector(String stageName, Map<String, Schema> inputSchemas) {
    this.stageName = stageName;
    this.inputSchemas = Collections.unmodifiableMap(new HashMap<>(inputSchemas));
    this.failures = new ArrayList<>();
  }

  @Override
  public ValidationFailure addFailure(String message, @Nullable String correctiveAction) {
    ValidationFailure failure = new ValidationFailure(message, correctiveAction, inputSchemas);
    failures.add(failure);
    return failure;
  }

  @Override
  public ValidationException getOrThrowException() throws ValidationException {
    if (failures.isEmpty()) {
      return new ValidationException(failures);
    }

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

    throw new ValidationException(failures);
  }

  public List<ValidationFailure> getValidationFailures() {
    return failures;
  }
}
