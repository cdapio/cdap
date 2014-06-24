package com.continuuity.explore.service;

import com.google.common.base.Objects;

/**
 * Represents the status of an operation submitted to {@link Explore}.
 */
public class Status {

  public static final Status NO_OP = new Status(OpStatus.FINISHED, false);

  private final OpStatus status;
  private final boolean hasResults;

  public Status(OpStatus status, boolean hasResults) {
    this.status = status;
    this.hasResults = hasResults;
  }

  public OpStatus getStatus() {
    return status;
  }

  public boolean hasResults() {
    return hasResults;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("state", status)
      .add("hasResults", hasResults)
      .toString();
  }

  /**
   * Represents the status of an operation.
   */
  @SuppressWarnings("UnusedDeclaration")
  public enum OpStatus {
    INITIALIZED,
    RUNNING,
    FINISHED,
    CANCELED,
    CLOSED,
    ERROR,
    UNKNOWN,
    PENDING
  }
}
