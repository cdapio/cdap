package com.continuuity.data.operation.executor;

/**
 * This represents a transaction in Omid. It has a transaction id, used as the write timestamp,
 * and a read pointer that provides read (snapshot) isolation.
 */
public class Transaction {
  private final long transactionId;
  private final ReadPointer readPointer;

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
