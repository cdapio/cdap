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
  private final boolean clearMeta;
  private final boolean clearTables;
  private final boolean clearQueues;
  private final boolean clearStreams;

  /**
   * clears everything (user data, queues, and streams).
   */
  public ClearFabric() {
    this(true, true, true, true, true);
  }

  /**
   * clears data, queues, and streams according to the specified boolean flags.
   * @param clearData whether to clear user data
   * @param clearMeta whether to clear meta data
   * @param clearTables whether to clear named tables
   * @param clearQueues whether to clear queues
   * @param clearStreams whether to clear streams
   */
  public ClearFabric(final boolean clearData,
                     final boolean clearMeta,
                     final boolean clearTables,
                     final boolean clearQueues,
                     final boolean clearStreams) {
    this.clearData = clearData;
    this.clearMeta = clearMeta;
    this.clearTables = clearTables;
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
   * Returns true if meta data should be cleared.
   * @return true if meta data should be cleared, false if not
   */
  public boolean shouldClearMeta() {
    return this.clearMeta;
  }

  /**
   * Returns true if named tables should be cleared.
   * @return true if named tables should be cleared, false if not
   */
  public boolean shouldClearTables() {
    return this.clearTables;
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
        .add("clearMeta", Boolean.toString(clearMeta))
        .add("clearQueues", Boolean.toString(clearQueues))
        .add("clearStreams", Boolean.toString(clearStreams))
        .toString();
  }

  @Override
  public long getId() {
    return id;
  }
}

