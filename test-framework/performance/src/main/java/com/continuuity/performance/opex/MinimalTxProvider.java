package com.continuuity.performance.opex;

import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.MinimalTxSystemClient;

/**
 * TxProvider that uses minimal tx service implementation with no perf overhead.
 */
public class MinimalTxProvider extends TxProvider {

  @Override
  TransactionSystemClient create() {
    return new MinimalTxSystemClient();
  }

}
