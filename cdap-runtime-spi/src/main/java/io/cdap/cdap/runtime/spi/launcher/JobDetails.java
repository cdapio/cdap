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

package io.cdap.cdap.runtime.spi.launcher;

import java.util.Objects;

/**
 * Represents launched job details.
 */
public class JobDetails {
  private final String status;

  public JobDetails(String status) {
    this.status = status;
  }

  /**
   * Returns status of the job.
   */
  public String getStatus() {
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
    JobDetails that = (JobDetails) o;
    return status.equals(that.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status);
  }
}
