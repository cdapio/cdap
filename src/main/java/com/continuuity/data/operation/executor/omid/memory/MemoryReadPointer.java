/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid.memory;

<<<<<<< HEAD
import java.util.Set;

=======
>>>>>>> master
import com.continuuity.data.operation.executor.ReadPointer;
import com.google.common.base.Objects;

import java.util.Set;

/**
 * A simple in-memory {@link ReadPointer} that supports a global read point,
 * an optional set of excludes, and an optional write pointer include. 
 */
public class MemoryReadPointer implements ReadPointer {
  final long readPoint;
  final long writePoint;
  final Set<Long> excludes;

  /**
   * Constructs a read pointer that will see all txids that are less than or
   * equal to the specified read point.
   * @param readPoint
   */
  public MemoryReadPointer(long readPoint) {
    this(readPoint, null);
  }

  /**
   * Constructs a read pointer that will see all txids that are less than or
   * equal to the specified read point and are not in the specified exclude
   * list.
   * @param readPoint
   * @param excludes
   */
  public MemoryReadPointer(long readPoint, Set<Long> excludes) {
    this(readPoint, -1L, excludes);
  }

  /**
   * Constructs a read pointer that will see all txids that are less than or
   * equal to the specified read point and are not in the specified exclude
   * list. Also adds a special exception for the specified write pointer which
   * can be used to allow a read-modify-write operation to see their own writes.
   * 
   * @param readPoint
   * @param writePoint
   * @param excludes
   */
  public MemoryReadPointer(long readPoint, long writePoint, Set<Long> excludes){
    this.readPoint = readPoint;
    this.writePoint = writePoint;
    this.excludes = excludes;
  }

  @Override
  public boolean isVisible(long txid) {
    if (txid == this.writePoint) return true;
    if (txid > this.readPoint) return false;
    return !isExcluded(txid);
  }

  private boolean isExcluded(long txid) {
    if (this.excludes != null && this.excludes.contains(txid)) return true;
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("readPoint", this.readPoint)
        .add("excludes", this.excludes)
        .toString();
  }

  @Override
  public long getMaximum() {
    return this.readPoint;
  }

  public long getWritePointer() {
    return this.writePoint;
  }

  public long getReadPointer() {
    return this.readPoint;
  }

  public Set<Long> getReadExcludes() {
    return this.excludes;
  }

  public static final MemoryReadPointer DIRTY_READ = new MemoryReadPointer(Long.MAX_VALUE);
}
