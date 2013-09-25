package com.continuuity.performance.tx;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.performance.benchmark.BenchmarkException;

/**
 * Abstract class for TxProvider.
 */
public abstract class TxProvider {

  /**
   * Consume the cmdline arguments it needs.
   * @param config configuration holding all options required by the tx service
   */
  void configure(CConfiguration config) throws BenchmarkException {
    // default implementations needs no configuration
  }

  /**
   * Create an operation executor.
   * @return the operation executor
   */
  abstract TransactionSystemClient create() throws BenchmarkException;

  /**
   * Shutdown the operation executor.
   * @param opex the operation executor that was previously obtained with
   *             create()
   */
  void shutdown(TransactionSystemClient opex) {
    // by default, do nothing
  }
}
