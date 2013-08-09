package com.continuuity.metrics.data;

import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.OmidTransactionException;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.TransactionResult;
import com.continuuity.data.operation.executor.omid.Undo;

import java.util.List;

/**
*
*/
public final class NoopTransactionOracle implements TransactionOracle {

  @Override
  public Transaction startTransaction(boolean trackChanges) {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void validateTransaction(Transaction tx) throws OmidTransactionException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void addToTransaction(Transaction tx, List<Undo> undos) throws OmidTransactionException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public TransactionResult commitTransaction(Transaction tx) throws OmidTransactionException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public TransactionResult abortTransaction(Transaction tx) throws OmidTransactionException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void removeTransaction(Transaction tx) throws OmidTransactionException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public ReadPointer getReadPointer() {
    throw new UnsupportedOperationException("Not supported");
  }
}
