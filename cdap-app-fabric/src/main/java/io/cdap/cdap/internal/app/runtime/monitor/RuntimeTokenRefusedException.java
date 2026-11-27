/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.util.Objects;

/**
 * Thrown when the Runtime Service terminally refuses a program run's runtime token (for example,
 * because the run requires a runtime token, the token belongs to a different run, or the run has
 * no persisted runtime scopes). Unlike transient HTTP errors, callers must not retry this failure.
 */
public final class RuntimeTokenRefusedException extends UnauthorizedException {

  private final String reason;

  RuntimeTokenRefusedException(RuntimeValidationFailure failure, String message) {
    this(Objects.requireNonNull(failure, "failure").getReason(), message);
  }

  RuntimeTokenRefusedException(String reason, String message) {
    super(message);
    String trimmed = Objects.requireNonNull(reason, "reason").trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException("reason must not be empty");
    }
    this.reason = trimmed;
  }

  public String getReason() {
    return reason;
  }
}
