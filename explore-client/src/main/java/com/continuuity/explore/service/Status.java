package com.continuuity.explore.service;

import com.google.common.base.Objects;

/**
 * Represents the status of an operation submitted to {@link Explore}.
 */
public class Status {
  private final State state;
  private final boolean hasResults;

  public Status(State state, boolean hasResults) {
    this.state = state;
    this.hasResults = hasResults;
  }

  public State getState() {
    return state;
  }

  public boolean hasResults() {
    return hasResults;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("state", state)
      .add("hasResults", hasResults)
      .toString();
  }

  /**
   * Represents the state of an operation.
   */
  @SuppressWarnings("UnusedDeclaration")
  public enum State {
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
