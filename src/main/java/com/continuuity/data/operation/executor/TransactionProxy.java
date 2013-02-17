package com.continuuity.data.operation.executor;

/**
 * The purpose of the transaction proxy is to provide a new TransactionAgent to a data set
 * for every invocation of the enclosing program entity. For instance, in a flowlet that uses
 * multiple data sets, we make sure (by injection) that all data sets go through the same proxy
 * for all their operations. Every time a new tuple is processed, the flowlet runner sets a
 * new transaction agent in the proxy. That way, all data sets start submitting the same
 * new transaction. Similarly for procedures.
 *
 */
public class TransactionProxy {

  private TransactionAgent agent = null;

  public void setTransactionAgent(TransactionAgent agent) {
    this.agent = agent;
  }

  public TransactionAgent getTransactionAgent() {
    return this.agent;
  }
}
