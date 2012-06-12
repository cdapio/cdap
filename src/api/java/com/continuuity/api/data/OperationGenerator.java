package com.continuuity.api.data;


public interface OperationGenerator<T> {

  public WriteOperation generateWriteOperation(T t);

}
