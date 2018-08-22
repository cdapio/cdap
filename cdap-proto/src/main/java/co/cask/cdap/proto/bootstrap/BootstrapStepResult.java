/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.proto.bootstrap;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Result of executing a bootstrap step.
 */
public class BootstrapStepResult {
  private final String label;
  private final Status status;
  private final String message;

  public BootstrapStepResult(String label, Status status) {
    this(label, status, null);
  }

  public BootstrapStepResult(String label, Status status, @Nullable String message) {
    this.label = label;
    this.status = status;
    this.message = message;
  }

  public String getLabel() {
    return label;
  }

  public Status getStatus() {
    return status;
  }

  @Nullable
  public String getMessage() {
    return message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BootstrapStepResult that = (BootstrapStepResult) o;
    return Objects.equals(label, that.label) &&
      status == that.status &&
      Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(label, status, message);
  }

  @Override
  public String toString() {
    return "BootstrapStepResult{" +
      "label='" + label + '\'' +
      ", status=" + status +
      ", message='" + message + '\'' +
      '}';
  }

  /**
   * Status of the bootstrap step
   */
  public enum Status {
    SUCCEEDED,
    FAILED,
    SKIPPED
  }
}
