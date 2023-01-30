/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.proto.sourcecontrol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Throw an exception when the RepositoryConfig is invalid.
 */
public class InvalidRepositoryConfigException extends RuntimeException {
  private final List<RepositoryValidationFailure> failures;

  public InvalidRepositoryConfigException(Collection<RepositoryValidationFailure> failures) {
    super(generateMessage(failures));
    this.failures = Collections.unmodifiableList(new ArrayList<>(failures));
  }

  public InvalidRepositoryConfigException(String message) {
    super(message);
    this.failures = Collections.unmodifiableList(new ArrayList<>());
  }

  /**
   * Returns a list of repository validation failures.
   */
  public List<RepositoryValidationFailure> getFailures() {
    return failures;
  }

  private static String generateMessage(Collection<RepositoryValidationFailure> failures) {
    String errorMessage = failures.isEmpty() ? "" : failures.iterator().next().getMessage();
    
    return String.format("Errors while validating repository configuration: %s", errorMessage);
  }
}
