/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.common.metrics;

import com.google.common.base.Objects;

/**
 * Response returned from the processing of a metric.
 */
public class MetricResponse {

  /**
   * Status of a metric processing.
   */
  public enum Status {
    SUCCESS(0),
    FAILED(1),
    IGNORED(2),
    INVALID(3),
    SERVER_ERROR(4);
    private int code;

    Status(int code) {
      this.code = code;
    }

    public int getCode() {
      return code;
    }
  }

  /**
   * Stores status of processing a metric.
   */
  private final Status status;

  public MetricResponse(Status status) {
    this.status = status;
  }

  /**
   * Returns the status of processing of the request.
   *
   * @return status of processing the metric request.
   */
  public Status getStatus() {
    return status;
  }

  /**
   * String respresentation of MetricResponse object.
   *
   * @return string representation of MetricResponse.
   */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("Status", status)
      .toString();
  }

}
