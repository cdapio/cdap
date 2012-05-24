/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid.memory;

import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.operation.executor.omid.RowSet;
import com.google.common.base.Objects;

/**
 * A set of rows (byte arrays).
 */
public class MemoryRowSet implements RowSet {

  private Set<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

  @Override
  public void addRow(byte [] row) {
    this.rows.add(row);
  }

  @Override
  public boolean contains(byte [] row) {
    return this.rows.contains(row);
  }

  @Override
  public boolean conflictsWith(RowSet rows) {
    for (byte [] row : this.rows) {
      if (rows.contains(row)) return true;
    }
    return false;
  }
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("numrows", rows.size())
        .add("rows", rows)
        .toString();
  }
}
