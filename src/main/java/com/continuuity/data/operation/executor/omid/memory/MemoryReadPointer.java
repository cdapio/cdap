/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid.memory;

import java.util.Set;

import com.continuuity.data.engine.ReadPointer;
import com.google.common.base.Objects;

/**
 * 
 */
public class MemoryReadPointer implements ReadPointer {
  final long readPoint;
  final Set<Long> excludes;
  public MemoryReadPointer(long readPoint) {
    this(readPoint, null);
  }
  public MemoryReadPointer(long readPoint, Set<Long> excludes) {
    this.readPoint = readPoint;
    this.excludes = excludes;
  }
  @Override
  public boolean isVisible(long txid) {
    if (txid > readPoint) return false;
    return !isExcluded(txid);
  }
  boolean isExcluded(long txid) {
    if (excludes != null && excludes.contains(txid)) return true;
    return false;
  }
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("readPoint", readPoint)
        .add("excludes", excludes)
        .toString();
  }
  @Override
  public long getMaximum() {
    return readPoint;
  }
}
