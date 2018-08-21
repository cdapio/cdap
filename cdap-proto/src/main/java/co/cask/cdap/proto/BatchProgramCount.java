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
 */

package co.cask.cdap.proto;

import co.cask.cdap.proto.id.ProgramId;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Result for the program count for http response
 */
public class BatchProgramCount extends BatchProgramResult {
  private final Long runCount;

  public BatchProgramCount(BatchProgram program, int statusCode, @Nullable String error, @Nullable Long runCount) {
    super(program, statusCode, error);
    this.runCount = runCount;
  }

  public BatchProgramCount(ProgramId programId, int statusCode, @Nullable String error, @Nullable Long runCount) {
    this(new BatchProgram(programId.getApplication(), programId.getType(), programId.getProgram()),
         statusCode, error, runCount);
  }

  /**
   * @return count of the program run. null if there is an error
   */
  @Nullable
  public Long getRunCount() {
    return runCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    BatchProgramCount that = (BatchProgramCount) o;

    return Objects.equals(runCount, that.runCount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), runCount);
  }
}
