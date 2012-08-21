package com.continuuity.common.benchmark;

public class BenchmarkException extends Exception {

  public BenchmarkException(String message) {
    super(message);
  }

  public BenchmarkException(String message, Exception cause) {
    super(message, cause);
  }

}
