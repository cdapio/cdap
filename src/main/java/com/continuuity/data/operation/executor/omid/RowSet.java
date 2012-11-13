/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid;


/**
 * A set of rows (byte arrays).
 */
public interface RowSet {

  public void addRow(byte [] row);

  public boolean contains(byte [] row);

  public boolean conflictsWith(RowSet rows);

}
