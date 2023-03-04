/*
 * Copyright © 2022 Cask Data, Inc.
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
import javax.annotation.Nullable;

/**
 * Used to send information about program status from RuntimeHandler to RuntimeClient.
 */
public class ProgramRunInfo {

  private final ProgramRunStatus programRunStatus;

  @Nullable
  private final Long terminateTs;

  /**
   * Creates a {@link ProgramRunInfo} in {@link ProgramRunStatus#STOPPING} state with the given
   * termination timestamp.
   *
   * @param terminateTs timestamp in seconds
   */
  ProgramRunInfo(long terminateTs) {
    this.programRunStatus = ProgramRunStatus.STOPPING;
    this.terminateTs = terminateTs;
  }

  /**
   * Creates a {@link ProgramRunInfo} with the given state except for {@link
   * ProgramRunStatus#STOPPING}, which should use {@link #ProgramRunInfo(long)} instead.
   *
   * @param programRunStatus the program run status
   */
  ProgramRunInfo(ProgramRunStatus programRunStatus) {
    if (programRunStatus == ProgramRunStatus.STOPPING) {
      throw new IllegalArgumentException(
          "The STOPPING state must associate with a termination timestamp.");
    }
    this.programRunStatus = programRunStatus;
    this.terminateTs = null;
  }

  public ProgramRunStatus getProgramRunStatus() {
    return programRunStatus;
  }

  /**
   * Returns the termination timestamp in seconds if the program run status is in {@link
   * ProgramRunStatus#STOPPING} state.
   */
  public long getTerminateTimestamp() {
    if (programRunStatus != ProgramRunStatus.STOPPING) {
      throw new IllegalStateException("Only the STOPPING state has termination timestamp");
    }
    // This shouldn't happen
    if (terminateTs == null) {
      throw new IllegalStateException("Missing termination timestamp");
    }

    return terminateTs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProgramRunInfo that = (ProgramRunInfo) o;
    return Objects.equals(this.getProgramRunStatus(), that.getProgramRunStatus())
        && Objects.equals(this.getTerminateTimestamp(), that.getTerminateTimestamp());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProgramRunStatus(), getTerminateTimestamp());
  }

  @Override
  public String toString() {
    return "ProgramRunInfo"
        + "{programRunStatus=" + programRunStatus
        + ", terminateTs='" + terminateTs
        + '}';
  }
}
