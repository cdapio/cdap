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

import co.cask.cdap.proto.BasicThrowable;

import java.util.Objects;
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
    RUN_FAILED,
    KILLED
  }

  private final Status status;
  private final BasicThrowable throwable;

  public PreviewStatus(Status status, @Nullable BasicThrowable throwable) {
    this.status = status;
    this.throwable = throwable;
  }

  public Status getStatus() {
    return status;
  }

  /**
   * Return the {@link Throwable} if preview failed, otherwise {@code null} is returned.
   */
  @Nullable
  public BasicThrowable getThrowable() {
    return throwable;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("PreviewStatus{");
    sb.append("status=").append(status);
    sb.append(", failureMessage='").append(throwable).append('\'');
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PreviewStatus that = (PreviewStatus) o;

    return Objects.equals(this.status, that.status) &&
      Objects.equals(this.throwable, that.throwable);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, throwable);
  }
}
