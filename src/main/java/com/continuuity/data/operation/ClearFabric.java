package com.continuuity.data.operation;

import com.continuuity.api.data.Operation;
import com.continuuity.api.data.OperationBase;
import com.google.common.base.Objects;

/**
 * Administrative operation for clearing the data fabric with options for only
 * clearing user data, queues, and/or streams.
 */
public class ClearFabric implements Operation {

  /** Unique id for the operation */
  private final long id = OperationBase.getId();

  private final boolean clearData;

  private final boolean clearQueues;

  private final boolean clearStreams;

  /**
   * clears everything (user data, queues, and streams).
   */
  public ClearFabric() {
    this(true, true, true);
  }

  /**
   * clears data, queues, and streams according to the specified boolean flags.
   * @param clearData true to clear user data, false to not clear queues
   * @param clearQueues true to clear queues, false to not clear queues
   * @param clearStreams true to clear streams, false to not clear streams
   */
  public ClearFabric(final boolean clearData, final boolean clearQueues,
                     final boolean clearStreams) {
    this.clearData = clearData;
    this.clearQueues = clearQueues;
    this.clearStreams = clearStreams;
  }

  /**
   * Returns true if user data should be cleared.
   * @return true if user data should be cleared, false if not
   */
  public boolean shouldClearData() {
    return this.clearData;
  }

  /**
   * Returns true if queues should be cleared.
   * @return true if queues should be cleared, false if not
   */
  public boolean shouldClearQueues() {
    return this.clearQueues;
  }

  /**
   * Returns true if streams should be cleared.
   * @return true if streams should be cleared, false if not
   */
  public boolean shouldClearStreams() {
    return this.clearStreams;
  }

  public String toString() {
    return Objects.toStringHelper(this)
        .add("clearData", Boolean.toString(clearData))
        .add("clearQueues", Boolean.toString(clearQueues))
        .add("clearStreams", Boolean.toString(clearStreams))
        .toString();
  }

  @Override
  public long getId() {
    return id;
  }
}

