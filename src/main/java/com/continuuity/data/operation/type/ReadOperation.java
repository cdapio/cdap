package com.continuuity.data.operation.type;

/**
 * Read operations 
 */
public interface ReadOperation<T> extends Operation {

  public void setResult(T result);

  public T getResult();

}
