package com.continuuity.gateway;

import com.continuuity.data.operation.executor.OperationExecutor;

/**
 * This is the base class for all Accessors. An Accessor is a type of Connector
 * allows external clients to access the data persisted in the data fabric via
 * RPC calls.
 */
public abstract class Accessor extends Connector implements DataAccessor {

  /**
   * the data fabric executor to use for all data access.
   */
  protected OperationExecutor executor;

  @Override
  public void setExecutor(OperationExecutor executor) {
    this.executor = executor;
  }

  @Override
  public OperationExecutor getExecutor() {
    return this.executor;
  }
}

