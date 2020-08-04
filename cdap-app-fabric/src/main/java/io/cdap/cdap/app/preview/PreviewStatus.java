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

package io.cdap.cdap.app.preview;

import io.cdap.cdap.proto.BasicThrowable;

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
    WAITING(false),
    INIT(false),
    RUNNING(false),
    COMPLETED(true),
    DEPLOY_FAILED(true),
    RUN_FAILED(true),
    KILLED(true),
    KILLED_BY_TIMER(true);

    private final boolean endState;

    Status(boolean endState) {
      this.endState = endState;
    }

    public boolean isEndState() {
      return endState;
    }
  }

  private final Status status;
  private final BasicThrowable throwable;
  private final Long startTime;
  private final Long endTime;
  private final Integer positionInWaitingQueue;

  public PreviewStatus(Status status, @Nullable BasicThrowable throwable, @Nullable Long startTime,
                       @Nullable Long endTime) {
    this(status, throwable, startTime, endTime, null);
  }

  public PreviewStatus(Status status, @Nullable BasicThrowable throwable, @Nullable Long startTime,
                       @Nullable Long endTime, @Nullable Integer positionInWaitingQueue) {
    this.status = status;
    this.throwable = throwable;
    this.startTime = startTime;
    this.endTime = endTime;
    this.positionInWaitingQueue = positionInWaitingQueue;
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

  @Nullable
  public Long getStartTime() {
    return startTime;
  }

  @Nullable
  public Long getEndTime() {
    return endTime;
  }

  @Nullable
  public Integer getPositionInWaitingQueue() {
    return positionInWaitingQueue;
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

    PreviewStatus status1 = (PreviewStatus) o;
    return status == status1.status &&
      Objects.equals(throwable, status1.throwable) &&
      Objects.equals(startTime, status1.startTime) &&
      Objects.equals(endTime, status1.endTime) &&
      Objects.equals(positionInWaitingQueue, status1.positionInWaitingQueue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, throwable, startTime, endTime, positionInWaitingQueue);
  }
}
