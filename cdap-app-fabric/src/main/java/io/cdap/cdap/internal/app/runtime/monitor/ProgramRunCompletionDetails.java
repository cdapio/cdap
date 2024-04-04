/*
 * Copyright © 2024 Cask Data, Inc.
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

import io.cdap.cdap.proto.ProgramRunStatus;
import java.util.Objects;

/**
 * Information about the completion of a
 * {@link io.cdap.cdap.app.program.Program} run managed by a
 * {@link io.cdap.cdap.runtime.spi.runtimejob.RuntimeJob}.
 */
public class ProgramRunCompletionDetails {

  private final long endTimestamp;
  private final ProgramRunStatus endStatus;

  public ProgramRunCompletionDetails(long endTimestamp, ProgramRunStatus endStatus) {
    this.endTimestamp = endTimestamp;
    this.endStatus = endStatus;
  }

  public long getEndTimestamp() {
    return endTimestamp;
  }

  public ProgramRunStatus getEndStatus() {
    return endStatus;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ProgramRunCompletionDetails)) {
      return false;
    }
    ProgramRunCompletionDetails that = (ProgramRunCompletionDetails) o;
    return endTimestamp == that.endTimestamp && endStatus == that.endStatus;
  }

  @Override
  public int hashCode() {
    return Objects.hash(endTimestamp, endStatus);
  }
}
