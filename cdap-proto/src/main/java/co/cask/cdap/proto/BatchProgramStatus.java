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

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/**
 * Describes the status of a program, as returned by the batch status endpoint POST /namespaces/{namespace}/status.
 */
public class BatchProgramStatus extends BatchProgram {
  private final int statusCode;
  private final String status;
  private final String error;

  public BatchProgramStatus(BatchProgram program, int statusCode, @Nullable String status, @Nullable String error) {
    super(program.appId, program.programType, program.programId);
    Preconditions.checkArgument(status != null || error != null, "'status' or 'error' must be specified.");
    this.statusCode = statusCode;
    this.status = status;
    this.error = error;
  }

  /**
   * @return the status code when getting the status of the program.
   */
  public int getStatusCode() {
    return statusCode;
  }

  /**
   * @return the status of the program. Null if there was an error.
   */
  @Nullable
  public String getStatus() {
    return status;
  }

  /**
   * @return the error message if there was an error. Null if there was no error.
   */
  @Nullable
  public String getError() {
    return error;
  }
}
