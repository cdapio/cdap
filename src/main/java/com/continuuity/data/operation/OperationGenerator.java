package com.continuuity.data.operation;

import com.continuuity.data.operation.type.WriteOperation;

public interface OperationGenerator<T> {

  public WriteOperation generateWriteOperation(T t);

}
