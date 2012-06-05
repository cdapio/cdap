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
import com.google.inject.Inject;

public class MemoryOracle implements TransactionOracle {

  /**
   * This is the TimeStamp generator for this Oracle
   */
  @Inject
  private TimestampOracle timeOracle;

  /**
   * Global read pointer. This is initialized from the timeOracle before
   * any of our public methods get called.
   */
  long readPoint = -1;

  /**
   * This maintains a map of TransactionIDs to its Rows
   */
  TreeMap<Long,RowSet> rowSets = new TreeMap<Long,RowSet>();

  /**
   * This the list of in progress transactions
   */
  TreeSet<Long> inProgress = new TreeSet<Long>();

  @Override
  public synchronized ReadPointer getReadPointer(long writeTxid) {
    initialize();
    return new MemoryReadPointer(this.readPoint, writeTxid,
        new HashSet<Long>(this.inProgress));
  }

  @Override
  public synchronized ReadPointer getReadPointer() {
    initialize();
    return new MemoryReadPointer(this.readPoint,
        new HashSet<Long>(this.inProgress));
  }

  @Override
  public synchronized long getWriteTxid() {

    initialize();

    long txid = this.timeOracle.getTimestamp();
    this.inProgress.add(txid);
    return txid;
  }

  @Override
  public synchronized ImmutablePair<ReadPointer, Long> getNewPointer() {

    initialize();

    long writeTxid = getWriteTxid();
    return new ImmutablePair<ReadPointer,Long>(
        getReadPointer(writeTxid), writeTxid);
  }

  @Override
  public synchronized boolean commit(long txid, RowSet rows)
    throws OmidTransactionException {

    initialize();

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

    initialize();

    if (!this.inProgress.contains(txid))
      throw new OmidTransactionException("Transaction not in progress");
    moveReadPointer(txid);
  }


  private void moveReadPointer(long txid) {
    this.inProgress.remove(txid);
    this.readPoint = this.timeOracle.getTimestamp();
    System.out.println("Oracle: ReadPoint = " + readPoint);
  }

  /**
   * Initialize our Timestamp value. This is a pre condition for any of our
   * public methods.
   */
  private void initialize() {

    // Has our timestamp been set?
    if (readPoint == -1) {
      readPoint = timeOracle.getTimestamp();
    }
  }

}
