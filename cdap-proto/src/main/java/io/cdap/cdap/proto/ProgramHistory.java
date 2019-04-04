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

package io.cdap.cdap.proto;

import io.cdap.cdap.proto.id.ProgramId;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Result for getting run records for a program.
 */
public class ProgramHistory {
  private final ProgramId programId;
  private final List<RunRecord> runs;
  private final Exception exception;

  public ProgramHistory(ProgramId programId, List<RunRecord> runs, @Nullable Exception exception) {
    this.programId = programId;
    this.runs = runs;
    this.exception = exception;
  }

  public ProgramId getProgramId() {
    return programId;
  }

  public List<RunRecord> getRuns() {
    return runs;
  }

  @Nullable
  public Exception getException() {
    return exception;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProgramHistory that = (ProgramHistory) o;
    return Objects.equals(programId, that.programId) &&
      Objects.equals(runs, that.runs) &&
      Objects.equals(exception, that.exception);
  }

  @Override
  public int hashCode() {
    return Objects.hash(programId, runs, exception);
  }
}
