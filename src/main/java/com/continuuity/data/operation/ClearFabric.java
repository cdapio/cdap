package com.continuuity.data.operation;

import com.continuuity.api.data.Operation;
import com.continuuity.api.data.OperationBase;
import com.google.common.base.Objects;

import java.util.Collections;
import java.util.List;

/**
 * Administrative operation for clearing the data fabric with options for only
 * clearing user data, queues, and/or streams.
 */
public class ClearFabric implements Operation {

  public enum ToClear {
    DATA, META, TABLES, QUEUES, STREAMS, ALL
  }

  /** Unique id for the operation */
  private final long id = OperationBase.getId();

  private boolean clearData;
  private boolean clearMeta;
  private boolean clearTables;
  private boolean clearQueues;
  private boolean clearStreams;

  /**
   * clears everything (user data, tables, meta data, queues, and streams).
   */
  public ClearFabric() {
    this(ToClear.ALL);
  }

  /**
   * clears the given scope (user data, tables, meta data, queues,
   * streams, or all).
   */
  public ClearFabric(ToClear whatToClear) {
    this(Collections.singletonList(whatToClear));
  }

  /**
   * clears the listed scopes (user data, tables, meta data, queues,
   * streams, or all).
   */
  public ClearFabric(List<ToClear> whatToClear) {
    clearData = clearMeta = clearTables = clearQueues =
        clearStreams = false;
    for (ToClear toClear : whatToClear) {
      switch (toClear) {
        case ALL: clearData = clearMeta = clearTables = clearQueues =
            clearStreams = true;
          break;
        case DATA: clearData = true; break;
        case META: clearMeta = true; break;
        case TABLES: clearTables = true; break;
        case QUEUES: clearQueues = true; break;
        case STREAMS: clearStreams = true; break;
      }
    }
  }

  /**
   * clears data, queues, and streams according to the specified boolean flags.
   * @param clearData whether to clear user data
   * @param clearMeta whether to clear meta data
   * @param clearTables whether to clear named tables
   * @param clearQueues whether to clear queues
   * @param clearStreams whether to clear streams
   */
  @Deprecated
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

