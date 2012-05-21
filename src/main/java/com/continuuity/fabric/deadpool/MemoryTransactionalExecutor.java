package com.continuuity.fabric.deadpool;

import com.continuuity.data.operation.executor.omid.memory.MemoryRowTracker;
import com.continuuity.fabric.engine.memory.MemoryOmidTransactionEngine;
import com.continuuity.fabric.engine.memory.MemoryOmidTransactionEngine.Oracle;

public class MemoryTransactionalExecutor implements NativeTransactionalExecutor {

  private final MemoryTransactionalEngine engine;

  private final Oracle oracle;
  
  private final MemoryRowTracker rowTracker;
  
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
  

}





