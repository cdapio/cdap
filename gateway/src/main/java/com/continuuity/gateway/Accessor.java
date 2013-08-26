package com.continuuity.gateway;

import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.weave.filesystem.LocationFactory;

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

  /**
   * the data fabric location factory to use for data access.
   */
  protected LocationFactory locationFactory;

  // to support early integration with TxDs2
  private DataSetAccessor dataSetAccessor;

  // to support early integration with TxDs2
  private TransactionSystemClient txSystemClient;

  @Override
  public void setExecutor(OperationExecutor executor) {
    this.executor = executor;
  }

  @Override
  public OperationExecutor getExecutor() {
    return this.executor;
  }

  @Override
  public void setLocationFactory(LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
  }

  @Override
  public LocationFactory getLocationFactory() {
    return this.locationFactory;
  }

  @Override
  public DataSetAccessor getDataSetAccessor() {
    return dataSetAccessor;
  }

  @Override
  public void setDataSetAccessor(DataSetAccessor dataSetAccessor) {
    this.dataSetAccessor = dataSetAccessor;
  }

  @Override
  public TransactionSystemClient getTxSystemClient() {
    return txSystemClient;
  }

  @Override
  public void setTxSystemClient(TransactionSystemClient txSystemClient) {
    this.txSystemClient = txSystemClient;
  }
}

