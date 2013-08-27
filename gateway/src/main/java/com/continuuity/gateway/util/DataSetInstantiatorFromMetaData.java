package com.continuuity.gateway.util;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.dataset.DataSetInstantiationBase;
import com.continuuity.data.dataset.DataSetInstantiationException;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SynchronousTransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * This is an instantiator that looks up each data set name in the meta
 * data service, tries to deserialize its specification from the meta data
 * and passes that spec on to a plain instantiator.
 */
public final class DataSetInstantiatorFromMetaData {

  // the operation executor
  private final OperationExecutor opex;
  // the location factory
  private LocationFactory locationFactory;

  // to support early integration with TxDs2
  private DataSetAccessor dataSetAccessor;

  // to support early integration with TxDs2
  private TransactionSystemClient txSystemClient;

  // the data set instantiator that will do the actual work
  private final DataSetInstantiationBase instantiator;
  // the meta data service
  private final MetadataService mds;

  /**
   * Constructor to use for read/write mode.
   *
   * @param opex the operation executor to use for data access
   * @param mds  the meta data store to use for meta data access
   */
  public DataSetInstantiatorFromMetaData(OperationExecutor opex, LocationFactory locationFactory,
                                         DataSetAccessor dataSetAccessor, TransactionSystemClient txSystemClient,
                                         MetadataService mds) {
    // set up the data set instantiator
    this.instantiator = new DataSetInstantiationBase();
    // we don't set the data set specs of the instantiator, instead we will
    // do that on demand every time getDataSet() is called

    // create an instance of meta data service
    this.mds = mds;
    this.opex = opex;
    this.locationFactory = locationFactory;
    this.dataSetAccessor = dataSetAccessor;
    this.txSystemClient = txSystemClient;
  }

  public <T extends DataSet> T getDataSet(String name, OperationContext context)
    throws DataSetInstantiationException {

    synchronized (this) {
      if (!this.instantiator.hasDataSet(name)) {
        // get the data set spec from the meta data store
        Dataset dsMeta;
        try {
          dsMeta = this.mds.getDataset(
            new Account(context.getAccount()),
            new Dataset(name));
        } catch (Exception e) {
          throw new DataSetInstantiationException(
            "Error reading data set spec for '" + name + "' from meta data service.", e);
        }
        if (!dsMeta.isExists()) {
          throw new DataSetInstantiationException(
            "Data set '" + name + "' not found in meta data service.");
        }
        String jsonSpec = dsMeta.getSpecification();
        if (jsonSpec == null || jsonSpec.isEmpty()) {
          throw new DataSetInstantiationException(
            "Data set '" + name + "' has no specification in meta data service.");
        }
        try {
          DataSetSpecification spec =
            new Gson().fromJson(jsonSpec, DataSetSpecification.class);
          this.instantiator.addDataSet(spec);
        } catch (JsonSyntaxException e) {
          throw new DataSetInstantiationException(
            "Error deserializing data set spec for '" + name + "' from JSON in meta data service.", e);
        }
      }
    }
    // create a new transaction proxy
    TransactionProxy proxy = new TransactionProxy();
    // set the transaction agent to synchronous
    proxy.setTransactionAgent(new SynchronousTransactionAgent(opex, context,
                                                              instantiator.getTransactionAware(),
                                                              txSystemClient));
    // this just gets passed through to the data set instantiator
    return this.instantiator.getDataSet(name,
                                        new DataFabricImpl(opex, locationFactory, dataSetAccessor, context),
                                        proxy);
  }

  // used only for unit-tests
  public DataSetInstantiationBase getInstantiator() {
    return instantiator;
  }
}
