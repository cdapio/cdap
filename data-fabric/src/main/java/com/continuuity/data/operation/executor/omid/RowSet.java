/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid;


import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * A set of rows (byte arrays).
 */
public interface RowSet extends Iterable<RowSet.Row> {

  /**
   * Represents the row of a write operation. Currently, a table name and a row key.
   */
  public static class Row implements Comparable<Row> {
    private String table;
    private byte[] row;

    /**
     * Create a new row from table name and row key.
     * @param table the table name, may be null
     * @param row the row key, must not be null
     */
    Row(String table, byte[] row) {
      this.table = table;
      this.row = row;
    }

    @Override
    public int compareTo(Row other) {
      if (this.table == null) {
        // this has no table
        if (other.table != null) {
          // but other has a table -> less
          return -1;
        }
      } else if (other.table == null) {
        // this has table but other does not -> greater
        return 1;
      } else {
        // both have a table
        int comp = this.table.compareTo(other.table);
        if (comp != 0) {
          // and they are different tables -> use table name order
          return comp;
        }
      }
      // same tables, compare rows
      return Bytes.compareTo(this.row, other.row);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(this.table, this.row);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other instanceof Row) {
        Row row = (Row) other;
         return Objects.equal(this.table, row.table) && Bytes.equals(this.row, row.row);
      }
      return false;
    }

    @Override
    public String toString() {
      return (this.table == null ? "<default>" : this.table) + ":" + Arrays.toString(this.row);
    }
  }

  /**
   * Add a row to this set.
   * @param row the new row
   */
  public void addRow(Row row);

  /**
   * Detect write conflicts, that is, at least one common row.
   * @param rows an other row set
   * @return whether the other row set has at least one row in common with this row set
   */
  public boolean conflictsWith(RowSet rows);
}
