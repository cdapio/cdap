package com.continuuity.data.operation.executor;

/**
 * This represents a transaction in Omid. It has a transaction id, used as the write timestamp,
 * and a read pointer that provides read (snapshot) isolation.
 */
public class Transaction {
<<<<<<< HEAD
  private long transactionId;
  private ReadPointer readPointer;
=======
  private final long transactionId;
  private final ReadPointer readPointer;
>>>>>>> master

  public Transaction(long transactionId, ReadPointer readPointer) {
    this.readPointer = readPointer;
    this.transactionId = transactionId;
  }

  public ReadPointer getReadPointer() {
    return readPointer;
  }

  public long getTransactionId() {
    return transactionId;
  }
}
