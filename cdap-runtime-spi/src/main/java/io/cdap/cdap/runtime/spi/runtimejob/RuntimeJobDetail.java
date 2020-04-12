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

package io.cdap.cdap.runtime.spi.runtimejob;

import io.cdap.cdap.runtime.spi.ProgramRunInfo;

import java.util.Objects;

/**
 * Represents runtime job details.
 */
public class RuntimeJobDetail {
  private final ProgramRunInfo runInfo;
  private final RuntimeJobStatus status;

  public RuntimeJobDetail(ProgramRunInfo runInfo, RuntimeJobStatus status) {
    this.runInfo = runInfo;
    this.status = status;
  }

  /**
   * Returns program run info.
   */
  public ProgramRunInfo getRunInfo() {
    return runInfo;
  }

  /**
   * Returns status of the job.
   */
  public RuntimeJobStatus getStatus() {
    return status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RuntimeJobDetail that = (RuntimeJobDetail) o;
    return runInfo.equals(that.runInfo) && status == that.status;
  }

  @Override
  public int hashCode() {
    return Objects.hash(runInfo, status);
  }
}
