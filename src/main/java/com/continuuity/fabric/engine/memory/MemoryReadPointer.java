/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.engine.memory;

import java.util.Set;

import com.continuuity.fabric.engine.transactions.ReadPointer;

/**
 * 
 */
public class MemoryReadPointer implements ReadPointer {
  final long readPoint;
  final Set<Long> excludes;
  MemoryReadPointer(long readPoint) {
    this(readPoint, null);
  }
  MemoryReadPointer(long readPoint, Set<Long> excludes) {
    this.readPoint = readPoint;
    this.excludes = excludes;
  }
  public boolean isVisible(long txid) {
    if (txid > readPoint) return false;
    return !isExcluded(txid);
  }
  boolean isExcluded(long txid) {
    if (excludes != null && excludes.contains(txid)) return true;
    return false;
  }
}
