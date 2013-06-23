/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid.memory;

import com.continuuity.data.operation.executor.omid.RowSet;
import com.google.common.base.Objects;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * A set of rows (byte arrays).
 */
public class MemoryRowSet implements RowSet {

  private Set<Row> rows = new TreeSet<Row>();

  @Override
  public void addRow(Row row) {
    this.rows.add(row);
  }

  // TODO this could be done more efficiently as a parallel scan
  @Override
  public boolean conflictsWith(RowSet other) {
    for (Row row : other) {
      if (this.rows.contains(row)) {
        return true;
      }
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

  @Override
  public Iterator<Row> iterator() {
    return rows.iterator();
  }
}
