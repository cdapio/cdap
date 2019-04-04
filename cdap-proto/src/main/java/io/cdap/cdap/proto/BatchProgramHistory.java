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

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Result for the batch program runs endpoint
 */
public class BatchProgramHistory extends BatchProgramResult {
  private final List<RunRecord> runs;

  public BatchProgramHistory(BatchProgram program, int statusCode, @Nullable String error, List<RunRecord> runs) {
    super(program, statusCode, error);
    this.runs = runs;
  }

  public List<RunRecord> getRuns() {
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
    if (!super.equals(o)) {
      return false;
    }

    BatchProgramHistory that = (BatchProgramHistory) o;

    return Objects.equals(runs, that.runs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), runs);
  }
}
