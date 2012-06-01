package com.continuuity.data.operation.executor.omid.memory;

import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.executor.omid.OmidTransactionException;
import com.continuuity.data.operation.executor.omid.RowSet;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.table.ReadPointer;

public class MemoryOracle implements TransactionOracle {

  final TimestampOracle timeOracle;

  long readPoint;

  TreeMap<Long,RowSet> rowSets = new TreeMap<Long,RowSet>();

  TreeSet<Long> inProgress = new TreeSet<Long>();

  public MemoryOracle(TimestampOracle timeOracle) {
    this.timeOracle = timeOracle;
    this.readPoint = this.timeOracle.getTimestamp();
  }

  @Override
  public synchronized ReadPointer getReadPointer() {
    return new MemoryReadPointer(this.readPoint,
        new HashSet<Long>(this.inProgress));
  }

  @Override
  public synchronized long getWriteTxid() {
    long txid = this.timeOracle.getTimestamp();
    this.inProgress.add(txid);
    return txid;
  }

  @Override
  public synchronized ImmutablePair<ReadPointer, Long> getNewPointer() {
    return new ImmutablePair<ReadPointer,Long>(
        getReadPointer(), getWriteTxid());
  }

  @Override
  public synchronized boolean commit(long txid, RowSet rows)
  throws OmidTransactionException {
    if (!this.inProgress.contains(txid))
      throw new OmidTransactionException("Transaction not in progress");
    // see if there are any conflicting rows between txid and now
    long now = this.timeOracle.getTimestamp();
    NavigableMap<Long,RowSet> rowsToCheck =
        this.rowSets.subMap(txid, false, now, false);
    for (Map.Entry<Long,RowSet> entry : rowsToCheck.entrySet()) {
      if (entry.getValue().conflictsWith(rows)) {
        return false;
      }
    }
    // No conflicts found!
    this.rowSets.put(now, rows);
    moveReadPointer(txid);
    return true;
  }

  @Override
  public synchronized void aborted(long txid) throws OmidTransactionException {
    if (!this.inProgress.contains(txid))
      throw new OmidTransactionException("Transaction not in progress");
    moveReadPointer(txid);
  }

  private void moveReadPointer(long txid) {
    this.inProgress.remove(txid);
    this.readPoint = this.timeOracle.getTimestamp();
    return;
  }
}
