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

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Result for the program count
 */
public class BatchProgramCount extends BatchProgramResult {
  private final int count;

  public BatchProgramCount(BatchProgram program, int statusCode, @Nullable String error, int count) {
    super(program, statusCode, error);
    this.count = count;
  }

  /**
   * @return the count of the program. -1 if there is an error
   */
  public int getCount() {
    return count;
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

    return Objects.equals(count, that.count);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), count);
  }
}
