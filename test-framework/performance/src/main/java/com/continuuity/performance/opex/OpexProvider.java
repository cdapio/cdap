package com.continuuity.performance.opex;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.performance.benchmark.BenchmarkException;

/**
 * Abstract class for OpexProvider.
 */
public abstract class OpexProvider {

  /**
   * Consume the cmdline arguments it needs.
   * @param config configuration holding all options required by the opex
   */
  void configure(CConfiguration config) throws BenchmarkException {
    // default implementations needs no configuration
  }

  /**
   * Create an operation executor.
   * @return the operation executor
   */
  abstract OperationExecutor create() throws BenchmarkException;

  /**
   * Shutdown the operation executor.
   * @param opex the operation executor that was previously obtained with
   *             create()
   */
  void shutdown(OperationExecutor opex) {
    // by default, do nothing
  }
}
