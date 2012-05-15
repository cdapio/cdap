package com.continuuity.fabric.operations;

public interface OperationGenerator<T> {

  public WriteOperation generateWriteOperation(T t);

}
