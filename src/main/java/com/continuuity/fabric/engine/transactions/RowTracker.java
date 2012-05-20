/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.engine.transactions;


/**
 *
 */
public interface RowTracker {

  public RowSet getRowSet(long txid);

  public RowSet setRowSet(long txid, RowSet rows);

  public RowSet deleteRowSet(long txid);
}
