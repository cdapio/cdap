package com.continuuity.api.data;

/**
 * Read operations 
 */
public interface ReadOperation<T> extends Operation {

  public void setResult(T result);

  public T getResult();

}
