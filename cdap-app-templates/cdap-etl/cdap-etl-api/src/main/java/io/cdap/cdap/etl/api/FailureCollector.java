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

package io.cdap.cdap.etl.api;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Failure collector is responsible to collect {@link ValidationFailure}s.
 */
@Beta
public interface FailureCollector {

  /**
   * Add a validation failure to this failure collector. The method returns the validation failure that was added to
   * the failure collector. This failure can be used to add additional {@link ValidationFailure.Cause}s.
   * For example,
   * <code>failureCollector.addFailure("message", "action").withConfigProperty("configProperty");</code>
   *
   * @param message failure message
   * @param correctiveAction corrective action
   * @return a validation failure
   * @throws UnsupportedOperationException if the implementation does not override this method
   */
  default ValidationFailure addFailure(String message, @Nullable String correctiveAction) {
    throw new UnsupportedOperationException("Adding a failure is not supported.");
  }

  /**
   * Throws validation exception if there are any failures that are added to the failure collector through
   * {@link #addFailure(String, String)}.
   * If no failures are added to the collector, it will return a {@link ValidationException} with empty failure list.
   *
   * <pre>
   *   String someMethod() {
   *   switch (someVar) {
   *     // cases
   *   }
   *   // if control comes here, it means failure
   *   failureCollector.addFailure(...);
   *   // throw validation exception so that compiler knows that exception is being thrown which eliminates the need to
   *   // have a statement that returns null towards the end of this method
   *   throw failureCollector.getOrThrowException();
   * }
   * </pre>
   *
   * @return returns a {@link ValidationException} if no failures were added to the collector
   * @throws ValidationException exception indicating validation failures
   * @throws UnsupportedOperationException if the implementation does not override this method
   */
  default ValidationException getOrThrowException() throws ValidationException {
    throw new UnsupportedOperationException("Throwing failures is not supported.");
  }

  /**
   * Get list of validation failures.
   *
   * @return list of validation failures
   */
  default List<ValidationFailure> getValidationFailures() {
    throw new UnsupportedOperationException("Getting failures is not supported.");
  }
}
