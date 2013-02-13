/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid;

import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.table.ReadPointer;

import java.util.List;


/**
 *
 */
public interface TransactionOracle {

  public ReadPointer getReadPointer();

  public ReadPointer getReadPointer(long writeTxid);

  public long getWriteTxid();
  
  public ImmutablePair<ReadPointer,Long> getNewPointer();
  
  public void add(long txid, List<Undo> undos) throws OmidTransactionException;

  public TransactionResult commit(long txid) throws OmidTransactionException;

  public TransactionResult abort(long txid) throws OmidTransactionException;

  void remove(long txid) throws OmidTransactionException;
}
