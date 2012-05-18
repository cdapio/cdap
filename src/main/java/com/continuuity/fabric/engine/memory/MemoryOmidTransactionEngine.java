/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.engine.memory;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.fabric.engine.memory.MemoryTransactionalExecutor.TransactionException;

/**
 * An in-memory transaction engine inspired by the Omid design.
 *
 * https://github.com/yahoo/omid
 */
public class MemoryOmidTransactionEngine {

  final Oracle oracle = new Oracle();

  final MemoryTransactionalEngine engine;

  public MemoryOmidTransactionEngine(MemoryTransactionalEngine engine) {
    this.engine = engine;
  }

  private final TreeMap<Long,RowSet> rowSets = new TreeMap<Long,RowSet>();

  public long startTransaction() {
    long txid = oracle.getWriteTxid();
    rowSets.put(txid, new RowSet());
    return txid;
  }

  public void write(byte [] key, byte [] value, long txid)
  throws TransactionException {
    RowSet rows = rowSets.get(txid);
    if (rows == null) throw new TransactionException(
        "Invalid transaction, not currently in progress");
    engine.write(key, value, txid);
    rows.addRow(key);
  }

  public byte [] read(byte [] key) {
    return engine.read(key, oracle.getReadTxid());
  }

  public boolean compareAndSwap(byte [] key, byte [] oldValue, byte [] newValue,
      long txid)
  throws TransactionException {
    RowSet rows = rowSets.get(txid);
    if (rows == null) throw new TransactionException(
        "Invalid transaction, not currently in progress");
    long readTxid = oracle.getReadTxid();
    boolean ret =
        engine.compareAndSwap(key, oldValue, newValue, readTxid, txid);
    rows.addRow(key);
    return ret;
  }

  void abortTransaction(long txid) throws TransactionException {
    RowSet rows = rowSets.get(txid);
    if (rows == null) throw new TransactionException(
        "Invalid transaction, not currently in progress");
    for (byte [] row : rows.rows) {
      engine.delete(row, txid);
    }
    oracle.aborted(txid);
  }

  public boolean commitTransaction(long txid) throws TransactionException {
    RowSet rows = rowSets.get(txid);
    if (rows == null) throw new TransactionException(
        "Invalid transaction, not currently in progress");
    boolean ret = oracle.commit(txid, rows);
    if (!ret) {
      abortTransaction(txid);
    }
    return ret;
  }

  class Oracle {

    private final TimestampOracle timestampOracle;

    long readPoint;

    TreeMap<Long,RowSet> rowSets = new TreeMap<Long,RowSet>();

    TreeSet<Long> inProgress = new TreeSet<Long>();

    Oracle() {
      this.timestampOracle = new TimestampOracle();
      this.readPoint = timestampOracle.getTimestamp();
    }

    public synchronized long getReadTxid() {
      return readPoint;
    }

    public synchronized long getWriteTxid() {
      long txid = timestampOracle.getTimestamp();
      inProgress.add(txid);
      return txid;
    }

    synchronized boolean commit(long txid, RowSet rows)
        throws TransactionException {
      if (!inProgress.contains(txid))
        throw new TransactionException("Transaction not in progress");
      // see if there are any conflicting rows between txid and now
      long now = timestampOracle.getTimestamp();
      NavigableMap<Long,RowSet> rowsToCheck =
          rowSets.subMap(txid, false, now, false);
      for (Map.Entry<Long,RowSet> entry : rowsToCheck.entrySet()) {
        if (entry.getValue().conflictsWith(rows)) {
          return false;
        }
      }
      // No conflicts found!
      rowSets.put(now, rows);
      moveReadPointer(txid);
      return true;
    }

    void aborted(long txid) throws TransactionException {
      if (!inProgress.contains(txid))
        throw new TransactionException("Transaction not in progress");
      moveReadPointer(txid);
    }

    private synchronized void moveReadPointer(long txid) {
      inProgress.remove(txid);
      if (inProgress.isEmpty()) {
        readPoint = timestampOracle.getTimestamp();
        return;
      }
      long first = inProgress.first();
      if (first < txid) return;
      else readPoint = first - 1;
    }

    private class TimestampOracle {
      final AtomicLong time = new AtomicLong(1);
      /** Returns a monotonically increasing "timestamp" (unrelated to time) */
      long getTimestamp() {
        return time.incrementAndGet();
      }
    }
  }

  class RowSet {

    Set<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

    void addRow(byte [] row) {
      rows.add(row);
    }

    private boolean contains(byte [] row) {
      return this.rows.contains(row);
    }

    public boolean conflictsWith(RowSet rows) {
      for (byte [] row : this.rows) {
        if (rows.contains(row)) return true;
      }
      return false;
    }
  }
}
