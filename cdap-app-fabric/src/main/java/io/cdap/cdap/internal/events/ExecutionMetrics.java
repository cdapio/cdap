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
 */

package io.cdap.cdap.internal.events;

import com.google.common.base.Objects;

/**
 * Metric details model for a single pipeline execution.
 */
public class ExecutionMetrics {

  private final int inputRows;
  private final int outputRows;
  private final long inputBytes;
  private final long outputBytes;

  public ExecutionMetrics(int inputRows, int outputRows, long inputBytes, long outputBytes) {
    this.inputRows = inputRows;
    this.outputRows = outputRows;
    this.inputBytes = inputBytes;
    this.outputBytes = outputBytes;
  }

  public static ExecutionMetrics emptyMetrics() {
    return new ExecutionMetrics(0, 0, 0, 0);
  }

  public int getInputRows() {
    return inputRows;
  }

  public int getOutputRows() {
    return outputRows;
  }

  public long getInputBytes() {
    return inputBytes;
  }

  public long getOutputBytes() {
    return outputBytes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExecutionMetrics that = (ExecutionMetrics) o;
    return inputRows == that.inputRows && outputRows == that.outputRows
      && inputBytes == that.inputBytes && outputBytes == that.outputBytes;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(inputRows, outputRows, inputBytes, outputBytes);
  }
}
