/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.engine.memory;

import java.util.TreeMap;

import com.continuuity.fabric.engine.transactions.RowSet;
import com.continuuity.fabric.engine.transactions.RowTracker;

/**
 *
 */
public class MemoryRowTracker implements RowTracker {

  final TreeMap<Long,RowSet> txRowSets = new TreeMap<Long,RowSet>();

  @Override
  public RowSet getRowSet(long txid) {
    return txRowSets.get(txid);
  }

  @Override
  public RowSet setRowSet(long txid, RowSet rows) {
    return txRowSets.put(txid, rows);
  }

  @Override
  public RowSet deleteRowSet(long txid) {
    return txRowSets.remove(txid);
  }
}
