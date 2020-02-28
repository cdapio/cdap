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

package io.cdap.cdap.proto;

import io.cdap.cdap.proto.id.ProgramId;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Result object for the batch call for fetching next/previous scheduled run time.
 */
public class BatchProgramSchedule extends BatchProgramResult {

  private final List<ScheduledRuntime> schedules;

  public BatchProgramSchedule(ProgramId programId, int statusCode, @Nullable String error,
                              @Nullable List<ScheduledRuntime> schedules) {
    super(programId.getApplication(), programId.getType(), programId.getProgram(), statusCode, error, null);
    this.schedules = schedules;
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
    BatchProgramSchedule that = (BatchProgramSchedule) o;
    return Objects.equals(schedules, that.schedules);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), schedules);
  }

  @Nullable
  public List<ScheduledRuntime> getSchedules() {
    return schedules;
  }
}
