package com.continuuity.explore.service;

import com.google.common.base.Objects;

/**
 * Represents the status of an operation submitted to {@link Explore}.
 */
public class Status {
  private final EStatus status;
  private final boolean hasResults;

  public Status(EStatus status, boolean hasResults) {
    this.status = status;
    this.hasResults = hasResults;
  }

  public EStatus getStatus() {
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
  public enum EStatus {
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
