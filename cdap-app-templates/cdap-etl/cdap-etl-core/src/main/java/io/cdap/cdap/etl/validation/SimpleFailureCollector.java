/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.validation;

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Failure collector which simply collects all the failures
 */
public class SimpleFailureCollector implements FailureCollector {

  private final List<ValidationFailure> failures;

  public SimpleFailureCollector() {
    this.failures = new ArrayList<>();
  }

  @Override
  public ValidationFailure addFailure(String message, @Nullable String correctiveAction) {
    ValidationFailure failure = new ValidationFailure(message, correctiveAction);
    failures.add(failure);
    return failure;
  }

  @Override
  public ValidationException getOrThrowException() throws ValidationException {
    if (failures.isEmpty()) {
      return new ValidationException(failures);
    }
    throw new ValidationException(failures);
  }

  public List<ValidationFailure> getValidationFailures() {
    return failures;
  }
}
