/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.engine.transactions;

import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * A set of rows (byte arrays).
 */
public class RowSet {

  private Set<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

  public void addRow(byte [] row) {
    this.rows.add(row);
  }

  private boolean contains(byte [] row) {
    return this.rows.contains(row);
  }

  public boolean conflictsWith(RowSet rows) {
    for (byte [] row : this.rows) {
      if (rows.contains(row)) return true;
    }
    return false;
  }
}
