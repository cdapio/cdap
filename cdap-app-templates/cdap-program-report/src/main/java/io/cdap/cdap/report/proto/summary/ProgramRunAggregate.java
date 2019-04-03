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

package co.cask.cdap.report.proto.summary;

import com.google.common.base.Objects;

/**
 * Represents an aggregate of program runs.
 */
abstract class ProgramRunAggregate {
  private final long runs;

  ProgramRunAggregate(long runs) {
    this.runs = runs;
  }

  /**
   * @return number of program runs in the aggregate
   */
  public long getRuns() {
    return runs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProgramRunAggregate that = (ProgramRunAggregate) o;
    return Objects.equal(runs, that.runs);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(runs);
  }
}
