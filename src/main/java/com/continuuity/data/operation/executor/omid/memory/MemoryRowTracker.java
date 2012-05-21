/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid.memory;

import java.util.TreeMap;

import com.continuuity.data.operation.executor.omid.RowTracker;

/**
 *
 */
public class MemoryRowTracker implements RowTracker {

  final TreeMap<Long,MemoryRowSet> txRowSets = new TreeMap<Long,MemoryRowSet>();

  @Override
  public MemoryRowSet getRowSet(long txid) {
    return txRowSets.get(txid);
  }

  @Override
  public MemoryRowSet setRowSet(long txid, MemoryRowSet rows) {
    return txRowSets.put(txid, rows);
  }

  @Override
  public MemoryRowSet deleteRowSet(long txid) {
    return txRowSets.remove(txid);
  }
}
