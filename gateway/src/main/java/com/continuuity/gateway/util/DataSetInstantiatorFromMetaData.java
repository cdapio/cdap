package com.continuuity.gateway.util;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.data.DataFabric2Impl;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.dataset.DataSetInstantiationBase;
import com.continuuity.data.dataset.DataSetInstantiationException;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.metadata.types.Dataset;
import com.continuuity.metadata.MetaDataStore;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;

/**
 * This is an instantiator that looks up each data set name in the meta
 * data service, tries to deserialize its specification from the meta data
 * and passes that spec on to a plain instantiator.
 */
public final class DataSetInstantiatorFromMetaData {

  // the location factory
  private LocationFactory locationFactory;

  // to support early integration with TxDs2
  private DataSetAccessor dataSetAccessor;

  // the data set instantiator that will do the actual work
  private final DataSetInstantiationBase instantiator;
  // the meta data service
  private final MetaDataStore mds;

  /**
   * Constructor to use for read/write mode.
   *
   * @param mds  the meta data store to use for meta data access
   */
  @Inject
  public DataSetInstantiatorFromMetaData(LocationFactory locationFactory,
                                         DataSetAccessor dataSetAccessor,
                                         MetaDataStore mds) {
    // set up the data set instantiator
    this.instantiator = new DataSetInstantiationBase();
    // we don't set the data set specs of the instantiator, instead we will
    // do that on demand every time getDataSet() is called

    // create an instance of meta data service
    this.mds = mds;
    this.locationFactory = locationFactory;
    this.dataSetAccessor = dataSetAccessor;
  }

  public <T extends DataSet> T getDataSet(String name, OperationContext context)
    throws DataSetInstantiationException {

    synchronized (this) {
      if (!this.instantiator.hasDataSet(name)) {
        // get the data set spec from the meta data store
        Dataset dsMeta;
        try {
          dsMeta = this.mds.getDataset(context.getAccount(), name);
        } catch (Exception e) {
          throw new DataSetInstantiationException(
            "Error reading data set spec for '" + name + "' from meta data service.", e);
        }
        if (dsMeta == null) {
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
    // this just gets passed through to the data set instantiator
    return this.instantiator.getDataSet(name, new DataFabric2Impl(locationFactory, dataSetAccessor));
  }

  // used only for unit-tests
  public DataSetInstantiationBase getInstantiator() {
    return instantiator;
  }
}
