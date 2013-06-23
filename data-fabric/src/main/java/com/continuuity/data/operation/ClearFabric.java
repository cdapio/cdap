package com.continuuity.data.operation;

import com.google.common.base.Objects;

import java.util.Collections;
import java.util.List;

/**
 * Administrative operation for clearing the data fabric with options for only
 * clearing user data, queues, and/or streams.
 */
public class ClearFabric extends Operation {

  /**
   * To specify what to clear.
   */
  public enum ToClear {
    DATA, META, TABLES, QUEUES, STREAMS, ALL
  }

  private boolean clearData;
  private boolean clearMeta;
  private boolean clearTables;
  private boolean clearQueues;
  private boolean clearStreams;

  /**
   * Clears everything (user data, tables, meta data, queues, and streams).
   */
  public ClearFabric() {
    this(ToClear.ALL);
  }

  /**
   * Clears the given scope (user data, tables, meta data, queues,
   * streams, or all).
   * @param whatToClear a scope to clear
   */
  public ClearFabric(ToClear whatToClear) {
    this(Collections.singletonList(whatToClear));
  }


  /**
   * Clears the listed scopes (user data, tables, meta data, queues,
   * streams, or all).
   * @param whatToClear list of scopes to clear
   */
  public ClearFabric(List<ToClear> whatToClear) {
    setToClear(whatToClear);
  }

  /**
   * Clears the listed scopes (user data, tables, meta data, queues,
   * streams, or all).
   * @param id explicit unique id of this operation
   * @param whatToClear list of scopes to clear
   */
  public ClearFabric(long id, List<ToClear> whatToClear) {
    super(id);
    setToClear(whatToClear);
  }

  private void setToClear(List<ToClear> whatToClear) {
    clearData = clearMeta = clearTables = clearQueues = clearStreams = false;
    for (ToClear toClear : whatToClear) {
      switch (toClear) {
        case ALL: clearData = clearMeta = clearTables = clearQueues = clearStreams = true;
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

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("clearData", Boolean.toString(clearData))
        .add("clearMeta", Boolean.toString(clearMeta))
        .add("clearQueues", Boolean.toString(clearQueues))
        .add("clearStreams", Boolean.toString(clearStreams))
        .toString();
  }
}

