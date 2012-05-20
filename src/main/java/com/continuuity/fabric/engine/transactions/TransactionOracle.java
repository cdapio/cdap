/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.engine.transactions;

/**
 * 
 */
public interface TransactionOracle {

  public ReadPointer getReadPointer();
  
  public long getWriteTxid();
  
  public boolean commit(long txid, RowSet rows);
  
  public void aborted(long txid);

}
