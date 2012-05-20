/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.engine.memory;

import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.fabric.engine.memory.MemoryTransactionalExecutor.TransactionException;
import com.continuuity.fabric.engine.transactions.ReadPointer;

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

  private final TreeMap<Long,RowSet> inProgress = new TreeMap<Long,RowSet>();

  public long startTransaction() {
    long txid = this.oracle.getWriteTxid();
    this.inProgress.put(txid, new RowSet());
    return txid;
  }

  public void write(byte [] key, byte [] value, long txid)
  throws TransactionException {
    RowSet rows = this.inProgress.get(txid);
    if (rows == null) throw new TransactionException(
        "Invalid transaction, not currently in progress");
    this.engine.write(key, value, txid);
    rows.addRow(key);
  }

  public byte [] read(byte [] key) {
    return this.engine.read(key, this.oracle.getReadPointer());
  }

  byte [] read(byte [] key, long readTxid) {
    return this.engine.read(key, new MemoryReadPointer(readTxid));
  }

  public boolean compareAndSwap(byte [] key, byte [] oldValue, byte [] newValue,
      long txid)
  throws TransactionException {
    RowSet rows = this.inProgress.get(txid);
    if (rows == null) throw new TransactionException(
        "Invalid transaction, not currently in progress");
    ReadPointer readPointer = this.oracle.getReadPointer();
    boolean ret =
        this.engine.compareAndSwap(key, oldValue, newValue, readPointer, txid);
    rows.addRow(key);
    return ret;
  }

  void abortTransaction(long txid) throws TransactionException {
    RowSet rows = this.inProgress.get(txid);
    if (rows == null) throw new TransactionException(
        "Invalid transaction, not currently in progress");
    for (byte [] row : rows.rows) {
      this.engine.delete(row, txid);
    }
    this.inProgress.remove(txid);
    this.oracle.aborted(txid);
  }

  public boolean commitTransaction(long txid) throws TransactionException {
    RowSet rows = this.inProgress.get(txid);
    if (rows == null) throw new TransactionException(
        "Invalid transaction, not currently in progress");
    boolean ret = this.oracle.commit(txid, rows);
    if (!ret) {
      abortTransaction(txid);
    } else {
      this.inProgress.remove(txid);
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
      this.readPoint = this.timestampOracle.getTimestamp();
    }

    public synchronized ReadPointer getReadPointer() {
      return new MemoryReadPointer(this.readPoint, new HashSet<Long>(inProgress));
    }

    public synchronized long getWriteTxid() {
      long txid = this.timestampOracle.getTimestamp();
      this.inProgress.add(txid);
      return txid;
    }

    synchronized boolean commit(long txid, RowSet rows)
        throws TransactionException {
      if (!this.inProgress.contains(txid))
        throw new TransactionException("Transaction not in progress");
      // see if there are any conflicting rows between txid and now
      long now = this.timestampOracle.getTimestamp();
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

    void aborted(long txid) throws TransactionException {
      if (!this.inProgress.contains(txid))
        throw new TransactionException("Transaction not in progress");
      moveReadPointer(txid);
    }

    private synchronized void moveReadPointer(long txid) {
      this.inProgress.remove(txid);
      this.readPoint = this.timestampOracle.getTimestamp();
      return;
    }

    private class TimestampOracle {
      final AtomicLong time = new AtomicLong(1);
      /** Returns a monotonically increasing "timestamp" (unrelated to time) */
      long getTimestamp() {
        return this.time.incrementAndGet();
      }
    }
  }

  class RowSet {

    Set<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

    void addRow(byte [] row) {
      this.rows.add(row);
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
