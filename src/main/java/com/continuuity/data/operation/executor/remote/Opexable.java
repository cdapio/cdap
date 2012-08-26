package com.continuuity.data.operation.executor.remote;

public interface Opexable<T> {

  public T call(OperationExecutorClient client);

}
