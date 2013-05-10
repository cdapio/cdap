package com.continuuity.performance.benchmark;

/**
 * Exception raised during execution of a Benchmark.
 */
public class BenchmarkException extends Exception {

  public BenchmarkException(String message) {
    super(message);
  }

  public BenchmarkException(String message, Exception cause) {
    super(message, cause);
  }

}
