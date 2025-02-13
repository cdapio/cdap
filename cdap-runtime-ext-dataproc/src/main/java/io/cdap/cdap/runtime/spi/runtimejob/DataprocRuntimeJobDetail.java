/*
 * Copyright © 2022 Cask Data, Inc.
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
 * Status details of dataproc runtime job.
 */
public class DataprocRuntimeJobDetail extends RuntimeJobDetail {

  private final String jobStatusDetails;
  private final String jobId;

  public DataprocRuntimeJobDetail(ProgramRunInfo runInfo, RuntimeJobStatus status,
      String jobStatusDetails, String jobId) {
    super(runInfo, status);
    this.jobStatusDetails = jobStatusDetails;
    this.jobId = jobId;
  }

  /**
   * Returns string representation of dataproc job status details.
   */
  public String getJobStatusDetails() {
    return jobStatusDetails;
  }

  /**
   * Returns dataproc job id.
   */
  public String getJobId() {
    return jobId;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    if (this.getClass() != o.getClass()) {
      return false;
    }

    DataprocRuntimeJobDetail that = (DataprocRuntimeJobDetail) o;
    return Objects.equals(jobStatusDetails, that.jobStatusDetails)
        && Objects.equals(jobId, that.jobId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getRunInfo(), this.getStatus(), jobStatusDetails, jobId);
  }
}
