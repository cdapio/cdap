package com.continuuity.api.data;

/**
 * This exception is thrown if - for whatever reason - a data set cannot be
 * instantiated at runtime.
 */
public class DataSetInstantiationException extends RuntimeException {

  public DataSetInstantiationException(String msg, Throwable e) {
    super(msg, e);
  }

  public DataSetInstantiationException(String msg) {
    super(msg);
  }
}
