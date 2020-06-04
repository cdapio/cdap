/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.join;

import io.cdap.cdap.etl.api.join.error.JoinError;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Thrown when a join definition is invalid.
 */
public class InvalidJoinException extends RuntimeException {
  private final Collection<JoinError> errors;

  public InvalidJoinException(Collection<JoinError> errors) {
    this(getMessage(errors), errors);
  }

  public InvalidJoinException(String message) {
    this(message, Collections.singletonList(new JoinError(message)));
  }

  public InvalidJoinException(String message, Collection<JoinError> errors) {
    super(message);
    this.errors = Collections.unmodifiableList(new ArrayList<>(errors));
  }

  public Collection<JoinError> getErrors() {
    return errors;
  }

  private static String getMessage(Collection<JoinError> errors) {
    if (errors.isEmpty()) {
      throw new IllegalStateException("An invalid join must contain at least one error, " +
                                        "or it must provide an error message.");
    }
    JoinError error = errors.iterator().next();
    String message = error.getMessage();
    return String.format("%s%s %s", message, message.endsWith(".") ? "" : ".", error.getCorrectiveAction());
  }
}
