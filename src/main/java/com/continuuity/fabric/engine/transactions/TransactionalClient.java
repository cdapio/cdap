/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.engine.transactions;

/**
 * 
 */
public class TransactionalClient {

  final TransactionOracle oracle;
  final TransactionalEngine engine;

  public TransactionalClient(TransactionOracle oracle,
      TransactionalEngine engine) {
    this.oracle = oracle;
    this.engine = engine;
    
  }
  
  
}
