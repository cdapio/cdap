package com.continuuity.data2.transaction;

/**
 * A factory for transaction executors.
 */
public interface TransactionExecutorFactory {

  TransactionExecutor createExecutor(Iterable<TransactionAware> txAwares);

}
