package com.continuuity.data.operation.executor.remote;

public interface Opexeptionable <T, E extends Exception> {

  public T call(OperationExecutorClient client) throws E;

}
