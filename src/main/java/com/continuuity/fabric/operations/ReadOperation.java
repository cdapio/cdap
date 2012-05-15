package com.continuuity.fabric.operations;

/**
 * Read operations 
 */
public interface ReadOperation<T> extends Operation {

  public void setResult(T result);

  public T getResult();

}
