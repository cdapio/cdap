package com.continuuity.data.operation.executor.omid.memory;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.omid.OmidTransactionException;
import com.continuuity.data.operation.executor.omid.RowSet;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.TransactionResult;
import com.continuuity.data.operation.executor.omid.Undo;
import com.continuuity.data.table.ReadPointer;
import com.google.inject.Inject;

import java.util.*;

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

  private static final Long[] LONG_TYPE = new Long[0];

  /**
   * This the list of in progress transactions
   */
  TreeMap<Long, List<Undo>> inProgress = new TreeMap<Long, List<Undo>>();

  @Override
  public synchronized ReadPointer getReadPointer(long writeTxid) {
    initialize();
    return new MemoryReadPointer(this.readPoint, writeTxid, this.inProgress.keySet());
  }

  @Override
  public synchronized ReadPointer getReadPointer() {
    initialize();
    return new MemoryReadPointer(this.readPoint, this.inProgress.keySet());
  }

  @Override
  public synchronized long getWriteTxid() {
    initialize();
    long txid = this.timeOracle.getTimestamp();
    this.inProgress.put(txid, new LinkedList<Undo>());
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
  public synchronized void add(long txid, List<Undo> undos) throws OmidTransactionException {
    initialize();
    List<Undo> existing = this.inProgress.get(txid);
    if (null == existing) {
      throw new OmidTransactionException(StatusCode.ILLEGAL_ADDTX,
                                         "Transaction not in progress");
    }
    existing.addAll(undos);
  }

  @Override
  public synchronized TransactionResult abort(long txid) throws OmidTransactionException {
    initialize();
    // test whether in progress
    List<Undo> undos = this.inProgress.get(txid);
    if (null == undos) {
      throw new OmidTransactionException(StatusCode.ILLEGAL_ABORT,
                                         "Transaction not in progress");
    }
    return new TransactionResult(undos);
  }

  @Override
  public synchronized void remove(long txid) throws OmidTransactionException {
    initialize();
    // test whether in progress
    List<Undo> undos = this.inProgress.get(txid);
    if (null == undos) {
      throw new OmidTransactionException(StatusCode.ILLEGAL_ABORT,
                                         "Transaction not in progress");
    }
    moveReadPointer(txid);
  }

  @Override
  public synchronized TransactionResult commit(long txid) throws OmidTransactionException {
    initialize();
    // test whether in progress
    List<Undo> undos = this.inProgress.get(txid);
    if (null == undos) {
      throw new OmidTransactionException(StatusCode.ILLEGAL_COMMIT,
                                         "Transaction not in progress");
    }
    // determine row set of transaction from undos
    RowSet rows = computeRowSet(undos);
    // see if there are any conflicting rows between txid and now - if the tx has written any rows
    if (rows != null) {
      long now = this.timeOracle.getTimestamp();
      NavigableMap<Long,RowSet> rowsToCheck =
        this.rowSets.subMap(txid, false, now, false);
      for (Map.Entry<Long,RowSet> entry : rowsToCheck.entrySet()) {
        if (entry.getValue().conflictsWith(rows)) {
          // TODO do we have to do more here?
          return new TransactionResult(undos);
        }
      }
      // No conflicts found, add to row sets
      this.rowSets.put(now, rows);
    }
    // remove this transaction from in-progress
    moveReadPointer(txid);

    // Find all row sets that were committed earlier than the start of
    // earliest transaction that's in progress and delete them from the
    // rowset.
    long minTxId = this.inProgress.isEmpty() ? Long.MAX_VALUE : this.inProgress.firstKey();
    SortedMap<Long, RowSet> toRemove = this.rowSets.headMap(minTxId);
    toRemove.clear(); // removes from the underlying map, this.rowSets

    // success
    return new TransactionResult();
  }

  private RowSet computeRowSet(List<Undo> undos) {
    RowSet rows = null;
    for (Undo undo : undos) {
      byte[] rowkey = undo.getRowKey();
      if (rowkey != null) {
        if (rows == null) {
          rows = new MemoryRowSet();
        }
        rows.addRow(rowkey);
      }
    }
    return rows;
  }

  public static boolean TRACE = false;
  
  private void moveReadPointer(long txid) {
    this.inProgress.remove(txid);
    this.readPoint = this.timeOracle.getTimestamp();
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
