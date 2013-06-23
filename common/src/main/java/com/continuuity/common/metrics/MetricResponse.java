package com.continuuity.common.metrics;

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
