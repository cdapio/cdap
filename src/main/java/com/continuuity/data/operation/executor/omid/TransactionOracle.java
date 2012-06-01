/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.table.ReadPointer;


/**
 *
 */
public interface TransactionOracle {

  public ReadPointer getReadPointer();

  public long getWriteTxid();
  
  public ImmutablePair<ReadPointer,Long> getNewPointer();
  
  public boolean commit(long txid, RowSet rows) throws OmidTransactionException;

  public void aborted(long txid) throws OmidTransactionException;

}
