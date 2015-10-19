/*
 * Copyright © 2015 Cask Data, Inc.
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
 * Describes the results returned by the batch stop/start endpoints POST /namespaces/{namespace}/stop|start.
 */
public class BatchProgramResult extends BatchProgram {
  private final int statusCode;
  private final String error;

  public BatchProgramResult(BatchProgram program, int statusCode, @Nullable String error) {
    this(program.appId, program.programType, program.programId, statusCode, error);
  }

  public BatchProgramResult(String appId, ProgramType programType, String programId,
                            int statusCode, @Nullable String error) {
    super(appId, programType, programId);
    this.statusCode = statusCode;
    this.error = error;
  }

  /**
   * @return the status code when getting the status of the program.
   */
  public int getStatusCode() {
    return statusCode;
  }

  /**
   * @return the error message if there was an error. Null if there was no error.
   */
  @Nullable
  public String getError() {
    return error;
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

    BatchProgramResult that = (BatchProgramResult) o;

    return Objects.equals(statusCode, that.statusCode) &&
      Objects.equals(error, that.error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), statusCode, error);
  }
}
