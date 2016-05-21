/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.api;

import javax.annotation.Nullable;

/**
 * Class to represent the state of the program.
 */
public class ProgramState {
  private final ProgramStatus status;
  private final String failureInfo;

  /**
   * Creates a new instance.
   * @param status status of the program
   * @param failureInfo cause of failure, null if the program execution is succeeded
   */
  public ProgramState(ProgramStatus status, @Nullable String failureInfo) {
    this.status = status;
    this.failureInfo = failureInfo;
  }

  /**
   * Return the {@link ProgramStatus} of the program.
   */
  public ProgramStatus getStatus() {
    return status;
  }

  /**
   * Return any available information on the reason of failure of the program.
   */
  @Nullable
  public String getFailureInfo() {
    return failureInfo;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ProgramState{");
    sb.append("status=").append(status);
    sb.append(", failureInfo='").append(failureInfo).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
