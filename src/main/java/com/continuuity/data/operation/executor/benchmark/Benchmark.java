package com.continuuity.data.operation.executor.benchmark;

import com.continuuity.data.operation.executor.OperationExecutor;

public abstract class Benchmark {

  public String[] configure(String[] args) throws BenchmarkException {
    // by default consume no arguments
    return args;
  }

  public void prepare(OperationExecutor opex) {
    // by default do nothing
  }

  public void warmup(OperationExecutor opex) {
    // by default do nothing
  }

  public void shutdown(OperationExecutor opex) {
    // by default do nothing
  }

  public void report(OperationExecutor opex) {
    // by default do nothing
  }

  public abstract AgentGroup[] getGroups(OperationExecutor opex);
}
