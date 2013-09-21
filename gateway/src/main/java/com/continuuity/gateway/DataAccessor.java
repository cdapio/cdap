package com.continuuity.gateway;

import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.weave.filesystem.LocationFactory;

/**
 * This is the base interface for all data accessors. An accessor is a type of
 * Connector that allows external clients to access the data persisted in the
 * data fabric via RPC calls. This interface ensures that all accessors have
 * common way to get the operations executor
 */
public interface DataAccessor {
  /**
   * Set the location factory to use for data fabric access.
   *
   * @param locationFactory the location factory to use
   */
  public void setLocationFactory(LocationFactory locationFactory);

  /*
   * Get the location factory to use for data fabric access.
   * @return the location factory to use
   */
  public LocationFactory getLocationFactory();

  // below methods are to support early integration with TxDs2

  DataSetAccessor getDataSetAccessor();

  void setDataSetAccessor(DataSetAccessor dataSetAccessor);

  TransactionSystemClient getTxSystemClient();

  void setTxSystemClient(TransactionSystemClient txSystemClient);
}
