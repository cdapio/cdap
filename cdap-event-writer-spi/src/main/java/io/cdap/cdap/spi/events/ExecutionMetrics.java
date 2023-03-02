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

package io.cdap.cdap.spi.events;

import java.util.Objects;

/**
 * Metric details model for a single pipeline execution.
 */
public class ExecutionMetrics {

  private final String stageId;
  private final int inputRows;
  private final int outputRows;
  private final long inputBytes;
  private final long outputBytes;
  private final int shuffleReadRecords;
  private final long shuffleReadBytes;
  private final int shuffleWriteRecords;
  private final long shuffleWriteBytes;

  public ExecutionMetrics(String stageId, int inputRows, int outputRows, long inputBytes,
      long outputBytes,
      int shuffleReadRecords, long shuffleReadBytes, int shuffleWriteRecords,
      long shuffleWriteBytes) {
    this.stageId = stageId;
    this.inputRows = inputRows;
    this.outputRows = outputRows;
    this.inputBytes = inputBytes;
    this.outputBytes = outputBytes;
    this.shuffleReadRecords = shuffleReadRecords;
    this.shuffleReadBytes = shuffleReadBytes;
    this.shuffleWriteRecords = shuffleWriteRecords;
    this.shuffleWriteBytes = shuffleWriteBytes;
  }

  public static ExecutionMetrics emptyMetrics() {
    return new ExecutionMetrics("0", 0, 0, 0, 0, 0, 0, 0, 0);
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

  public String getStageId() {
    return stageId;
  }

  public int getShuffleReadRecords() {
    return shuffleReadRecords;
  }

  public long getShuffleReadBytes() {
    return shuffleReadBytes;
  }

  public int getShuffleWriteRecords() {
    return shuffleWriteRecords;
  }

  public long getShuffleWriteBytes() {
    return shuffleWriteBytes;
  }

  @Override
  public String toString() {
    return "ExecutionMetrics{"
        + "stageId='" + stageId + '\''
        + ", inputRows=" + inputRows
        + ", outputRows=" + outputRows
        + ", inputBytes=" + inputBytes
        + ", outputBytes=" + outputBytes
        + ", shuffleReadRecords=" + shuffleReadRecords
        + ", shuffleReadBytes=" + shuffleReadBytes
        + ", shuffleWriteRecords=" + shuffleWriteRecords
        + ", shuffleWriteBytes=" + shuffleWriteBytes
        + '}';
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
        && inputBytes == that.inputBytes
        && outputBytes == that.outputBytes && shuffleReadRecords == that.shuffleReadRecords
        && shuffleReadBytes == that.shuffleReadBytes
        && shuffleWriteRecords == that.shuffleWriteRecords
        && shuffleWriteBytes == that.shuffleWriteBytes && Objects.equals(stageId, that.stageId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stageId, inputRows, outputRows, inputBytes, outputBytes,
        shuffleReadRecords, shuffleReadBytes, shuffleWriteRecords, shuffleWriteBytes);
  }
}
