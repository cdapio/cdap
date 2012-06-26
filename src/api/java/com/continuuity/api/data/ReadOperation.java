package com.continuuity.api.data;

/**
 * An {@link Operation} that reads and returns data.
 *
 * @param <T> return type of this read operation
 */
public interface ReadOperation<T> extends Operation {

  public void setResult(T result);

  public T getResult();

}
