/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid;

import com.continuuity.data.operation.executor.omid.memory.MemoryRowSet;


/**
 *
 */
public interface RowTracker {

  public MemoryRowSet getRowSet(long txid);

  public MemoryRowSet setRowSet(long txid, MemoryRowSet rows);

  public MemoryRowSet deleteRowSet(long txid);
}
