package com.continuuity.fabric.engine.memory;

import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import com.continuuity.fabric.engine.NativeTransactionalExecutor;

public class MemoryTransactionalExecutor implements NativeTransactionalExecutor {

  private final MemoryTransactionalEngine engine;

  private final AtomicLong txid = new AtomicLong(0);

  private final TreeSet<Transaction> txInProgress = new TreeSet<Transaction>();

  public MemoryTransactionalExecutor(MemoryTransactionalEngine engine) {
    this.engine = engine;
  }

  public Transaction startTransaction() {
    Transaction tx = new Transaction(txid.incrementAndGet());
    txInProgress.add(tx);
    return tx;
  }

  public void commitTransaction(Transaction tx) throws TransactionException {
    if (!txInProgress.contains(tx)) throw new TransactionException(
        "Transaction not in progress, unable to commit");
    txInProgress.remove(tx);
    
  }

  MemoryTransactionalEngine getEngine() {
    return engine;
  }
  
  public static class Transaction implements Comparable<Transaction> {
    final long txid;
    Transaction(long txid) {
      this.txid = txid;
    }
    /**
     * Orders in the usual ascending order of compareTo.
     */
    @Override
    public int compareTo(Transaction t) {
      if (txid < t.txid) return -1;
      if (txid > t.txid) return 1; 
      return 0;
    }
  }
  
  public static class TransactionException extends Exception {
    private static final long serialVersionUID = 7253274239927817754L;

    public TransactionException(String msg) {
      super(msg);
    }
  }

}





