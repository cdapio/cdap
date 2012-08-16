package com.continuuity.data.operation.executor.benchmark;

import com.continuuity.data.operation.executor.OperationExecutor;

public abstract class OpexProvider {

  /**
   * Consume the cmdline arguments it needs.
   * @param args command line arguments
   * @return the remaining arguments
   */
  String[] configure(String[] args) throws BenchmarkException {
    // default implementations consumes none of the arguments
    return args;
  }

  /**
   * Create an operation executor
   * @return the operation executor
   */
  abstract OperationExecutor create() throws BenchmarkException;

  /**
   * Shutdown the operation executor
   * @param opex the operation executor that was previously obtained with
   *             create()
   */
  void shutdown(OperationExecutor opex) throws BenchmarkException {
    // by default, do nothing
  }

}
