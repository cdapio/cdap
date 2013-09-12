package com.continuuity.data2.transaction;

/**
 * A factory for transaction executors.
 */
public interface TransactionExecutorFactory {

  DefaultTransactionExecutor createExecutor(Iterable<TransactionAware> txAwares);

}
