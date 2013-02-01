package com.continuuity.data.operation;

/**
 * Undelete of a Delete.
 */
public class Undelete extends Delete {

  public Undelete(String table, byte[] key) {
    super(table, key);
  }

  public Undelete(String table, byte[] row, byte[] column) {
    super(table, row, column);
  }

  public Undelete(String table, byte[] row, byte[][] columns) {
    super(table, row, columns);
  }

  public Undelete(byte[] key) {
    super(key);
  }
  
  public Undelete(byte[] row, byte [] column) {
    super(row, column);
  }

  public Undelete(byte [] row, byte [][] columns) {
    super(row, columns);
  }
}
