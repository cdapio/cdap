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

package co.cask.cdap.app.preview;

import javax.annotation.Nullable;

/**
 * Class representing status and failure reason (if any) associated with the Preview.
 */
public class PreviewStatus {
  /**
   * Status for the preview
   */
  public enum Status {
    RUNNING,
    COMPLETED,
    DEPLOY_FAILED,
    RUN_FAILED
  }

  private final Status status;
  private final String failureMessage;

  public PreviewStatus(Status status, @Nullable String failureMessage) {
    this.status = status;
    this.failureMessage = failureMessage;
  }

  public Status getStatus() {
    return status;
  }

  @Nullable
  public String getFailureMessage() {
    return failureMessage;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("PreviewStatus{");
    sb.append("status=").append(status);
    sb.append(", failureMessage='").append(failureMessage).append('\'');
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof PreviewStatus)) {
      return false;
    }

    PreviewStatus that = (PreviewStatus) o;

    if (failureMessage != null ? !failureMessage.equals(that.failureMessage) : that.failureMessage != null) {
      return false;
    }

    return status == that.status;
  }

  @Override
  public int hashCode() {
    int result = status.hashCode();
    result = 31 * result + (failureMessage != null ? failureMessage.hashCode() : 0);
    return result;
  }
}
