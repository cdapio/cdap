package com.continuuity.api.data;

/**
 * An {@link Operation} that reads and returns data.
 */
public interface ReadOperation<T> extends Operation {

  public void setResult(T result);

  public T getResult();

}
