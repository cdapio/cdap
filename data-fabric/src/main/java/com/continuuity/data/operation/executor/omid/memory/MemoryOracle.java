package com.continuuity.data.operation.executor.omid.memory;

import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.OmidTransactionException;
import com.continuuity.data.operation.executor.omid.QueueUndo;
import com.continuuity.data.operation.executor.omid.RowSet;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.TransactionResult;
import com.continuuity.data.operation.executor.omid.Undo;
import com.continuuity.data.operation.ttqueue.QueueFinalize;
import com.google.inject.Inject;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This implements an in-memory transaction oracle.
 */
public class MemoryOracle implements TransactionOracle {

  // TODO we must regularly check the in-progress transactions for timeouts.
  // TODO where should that be done? As part of start-transaction? Or as a background thread?

  /**
   * This is the TimeStamp generator for this Oracle.
   */
  @Inject
  private TimestampOracle timeOracle;

  /**
   * Global read pointer. This is maintained as greatest transaction id of all
   * committed transaction. There may be in-progress or invalid transactions
   * with a smaller id, but they are excluded from reads.
   */
  long readPoint = -1;

  /**
   * Move the read point after a transaction was committed. This is maintained as
   * greatest transaction id of all committed transactions. This it only needs to
   * be moved after a successful commit, not after an abort or failed commit.
   * @param committedTxid the id of the committed transaction
   */
  private void moveReadPoint(long committedTxid) {
    if (committedTxid > this.readPoint) {
      this.readPoint = committedTxid;
    }
  }

  /**
   * This maintains a map of TransactionIDs to its Rows.
   */
  TreeMap<Long, RowSet> rowSets = new TreeMap<Long, RowSet>();

  /**
   * This class represents an in-progress transaction. It has a timestamp
   * indicating the last time this transaction was active (it will timeout
   * after too much time), and a list of undo operations to rollback any
   * writes performed so far.
   */
  private static class InProgress {
    private AtomicLong timestamp;
    private List<Undo> undos;

    public AtomicLong getTimestamp() {
      return timestamp;
    }
    public List<Undo> getUndos() {
      return undos;
    }

    /**
     * Constructor that is called at the beginning of a transaction. It
     * initializes the timestamp to the current time and the undo operations
     * to empty.
     */
    private InProgress() {
      this.timestamp = new AtomicLong(System.currentTimeMillis());
      this.undos = new LinkedList<Undo>();
    }

    /**
     * Add a bunch of undo operations. This is called every time an operation
     * (or a batch of operations) is executed by the transaction.
     * @param undos the list of undo operations to add
     */
    public void add(List<Undo> undos) {
      this.undos.addAll(undos);
      this.timestamp.set(System.currentTimeMillis());
    }
  }

  /**
   * This the list of in progress transactions, each with a time stamp and a
   * list of undo operations, to rollback any writes performed so far.
   */
  NavigableMap<Long, InProgress> inProgress = new ConcurrentSkipListMap<Long, InProgress>();

  /**
   * Utility to get the in-progress status of a transaction, or throw an
   * exception if it is not in progress.
   */
  InProgress getInProgress(long txid) throws OmidTransactionException {
    InProgress existing = this.inProgress.get(txid);
    if (null == existing) {
      throw new OmidTransactionException(
        StatusCode.INVALID_TRANSACTION, "Transaction not in progress");
    }
    return existing;
  }

  /**
   * This is the set of transactions that are excluded from read, because
   * they are either in progress or invalid.
   */
  TreeSet<Long> excludes = new TreeSet<Long>();

  /**
   * Obtain the set of transactions that have to excluded from read,
   * either because they are invalid or because they are in progress.
   * @return a set of transaction ids
   */
  Set<Long> getExcludes() {
    return new TreeSet<Long>(this.excludes);
  }

  @Override
  public synchronized ReadPointer getReadPointer() {
    if (readPoint < 0) {
      readPoint = this.timeOracle.getTimestamp();
    }
    return new MemoryReadPointer(this.readPoint, this.getExcludes());
  }

  @Override
  public synchronized Transaction startTransaction(boolean trackChanges) {
    long txid = this.timeOracle.getTimestamp();
    this.inProgress.put(txid, new InProgress());
    Set<Long> excludes = this.getExcludes();
    this.excludes.add(txid);
    return new Transaction(txid, new MemoryReadPointer(txid, txid, excludes), trackChanges);
  }

  @Override
  public void validateTransaction(Transaction tx) throws OmidTransactionException {
    getInProgress(tx.getWriteVersion());
  }

  @Override
  public synchronized void addToTransaction(Transaction tx, List<Undo> undos) throws OmidTransactionException {
    // don't store undos for transaction that doesn't track changes
    if (tx.isTrackChanges()) {
      getInProgress(tx.getWriteVersion()).add(undos);
    }
  }

  @Override
  public synchronized TransactionResult abortTransaction(Transaction tx) throws OmidTransactionException {
    // this removes the txid from in-progress. It remains in the exclude
    // list until removed after successful undo of its writes.
    InProgress existing = this.inProgress.remove(tx.getWriteVersion());
    if (null == existing) {
      throw new OmidTransactionException(
        StatusCode.INVALID_TRANSACTION, "Transaction not in progress");
    }
    return new TransactionResult(existing.getUndos());
  }

  @Override
  public synchronized void removeTransaction(Transaction tx) throws OmidTransactionException {
    long txid = tx.getWriteVersion();
    boolean failed = false;
    // This is called after all writes of a failed transaction have been
    // undone successfully. In this case it removes the txid from the invalid and excluded list.
    // In case of transaction that doesn't track undos, we have to keep it in the list of excluded. Changes made by
    // such transaction will be cleaned up separately
    if (tx.isTrackChanges()) {
      failed = !this.excludes.remove(txid);
    }

    if (failed || this.inProgress.containsKey(txid)) {
      throw new OmidTransactionException(StatusCode.INVALID_TRANSACTION, "Transaction not aborted");
    }

    // we must move the read pointer. If the failed transaction performed an
    // enqueue, then it was undone by overwriting the entry meta data with an
    // invalid marker. That write must be visible to subsequent dequeue calls.
    // TODO revisit this after new queue implementation is done
    moveReadPoint(txid);
  }

  @Override
  public synchronized TransactionResult commitTransaction(Transaction tx) throws OmidTransactionException {
    long txid = tx.getWriteVersion();
    List<Undo> undos = getInProgress(txid).getUndos();
    // determine row set of transaction from undos
    RowSet rows = computeRowSet(undos);
    // if the tx has written any rows, we check for conflicts and remember its row set
    if (rows != null) {
      // a conflict exists iff a transaction that committed after the start of this
      // transaction has written any row that this transaction also wrote. We remember
      // the row set of each transaction with its end time. Thus, we need to check all
      // row sets that have an end time between the start of this transaction (the txid)
      // and the current oracle time.
      long now = this.timeOracle.getTimestamp();
      NavigableMap<Long, RowSet> rowsToCheck =
        this.rowSets.subMap(txid, false, now, false);
      for (Map.Entry<Long, RowSet> entry : rowsToCheck.entrySet()) {
        if (entry.getValue().conflictsWith(rows)) {
          // we have a conflict -> transaction failed
          return abortTransaction(tx);
        }
      }
      // No conflicts found, add to row sets
      this.rowSets.put(now, rows);
    }
    // remove this transaction from in-progress and from excludes
    this.inProgress.remove(txid);
    this.excludes.remove(txid);
    // and move the read point (only done after a successful commit).
    moveReadPoint(txid);

    // Find all row sets that were committed earlier than the start of
    // earliest transaction that's in progress and delete them from the
    // row set.
    long minTxId = this.inProgress.isEmpty() ? Long.MAX_VALUE : this.inProgress.firstKey();
    SortedMap<Long, RowSet> toRemove = this.rowSets.headMap(minTxId);
    toRemove.clear(); // removes from the underlying map, this.rowSets

    // success
    return new TransactionResult(undos, computeFinalize(undos));
  }

  /**
   * Utility to extract the row set of a transaction from its undos.
   * @param undos the undo operations for this transaction
   * @return a set of rows that were written by the transaction
   */
  private RowSet computeRowSet(List<Undo> undos) {
    RowSet rows = null;
    for (Undo undo : undos) {
      RowSet.Row rowKey = undo.getRow();
      if (rowKey != null) {
        if (rows == null) {
          rows = new MemoryRowSet();
        }
        rows.addRow(rowKey);
      }
    }
    return rows;
  }

  /**
   * Utility to look for a queue unack in the list of undos, and if found,
   * creates a corresponding queue finalize.
   * @return the generated queue finalize operation
   */
  private QueueFinalize computeFinalize(List<Undo> undos) {
    // assume that the unack is the last undo in the list - as the ack is
    // always the last operation in a batch (last thing a flowlet does).
    if (!undos.isEmpty()) {
      Undo last = undos.get(undos.size() - 1);
      if (last instanceof QueueUndo.QueueUnack) {
        QueueUndo.QueueUnack unack = (QueueUndo.QueueUnack) last;
        return new QueueFinalize(unack.getQueueName(), unack.getEntryPointers(),
                                 unack.getConsumer(), unack.getNumGroups());
      }
    }
    return null;
  }
}
