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

import java.util.Objects;

/**
 * Represents runtime job details.
 */
public class RuntimeJobDetail {
  private final RuntimeJobId runtimeJobId;
  private final RuntimeJobStatus status;

  public RuntimeJobDetail(RuntimeJobId runtimeJobId, RuntimeJobStatus status) {
    this.runtimeJobId = runtimeJobId;
    this.status = status;
  }

  /**
   * Returns status of the job.
   */
  public RuntimeJobStatus getStatus() {
    return status;
  }

  /**
   * Returns runtime job id.
   */
  public RuntimeJobId getRuntimeJobId() {
    return runtimeJobId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RuntimeJobDetail that = (RuntimeJobDetail) o;
    return runtimeJobId.equals(that.runtimeJobId) &&
      status == that.status;
  }

  @Override
  public int hashCode() {
    return Objects.hash(runtimeJobId, status);
  }
}
