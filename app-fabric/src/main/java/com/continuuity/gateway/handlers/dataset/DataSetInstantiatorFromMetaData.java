package com.continuuity.gateway.handlers.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetInstantiationException;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataFabric2Impl;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.dataset.DataSetInstantiationBase;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.dataset2.InMemoryDatasetFramework;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This is an instantiator that looks up each data set name in the meta
 * data service, tries to deserialize its specification from the meta data
 * and passes that spec on to a plain instantiator.
 */
public final class DataSetInstantiatorFromMetaData {

  private static final Logger LOG = LoggerFactory.getLogger(DataSetInstantiatorFromMetaData.class);

  // the location factory
  private LocationFactory locationFactory;

  // to support early integration with TxDs2
  private DataSetAccessor dataSetAccessor;

  // the data set instantiator that will do the actual work
  private final DataSetInstantiationBase instantiator;

  /**
   * Json serializer.
   */
  private static final Gson GSON = new Gson();

  private Store store;
  @Inject
  public DataSetInstantiatorFromMetaData(LocationFactory locationFactory, CConfiguration configuration,
                                         DataSetAccessor dataSetAccessor, StoreFactory storeFactory) {
    // set up the data set instantiator
    this.instantiator = new DataSetInstantiationBase(configuration);
    // we don't set the data set specs of the instantiator, instead we will
    // do that on demand every time getDataSet() is called

    this.locationFactory = locationFactory;
    this.dataSetAccessor = dataSetAccessor;
    this.store = storeFactory.create();
  }

  public <T extends DataSet> T getDataSet(String name, OperationContext context)
    throws DataSetInstantiationException {

    synchronized (this) {
      if (!this.instantiator.hasDataSet(name)) {
        DataSetSpecification spec = getDataSetSpecification(name, context);
        this.instantiator.addDataSet(spec);
      }
      // this just gets passed through to the data set instantiator
      // This call needs to be inside the synchronized call, otherwise it's possible that we are adding a DataSet
      // to the instantiator while retrieving an existing one (try to access while updating the underlying map).
      return this.instantiator.getDataSet(name, new DataFabric2Impl(locationFactory, dataSetAccessor),
                                          // NOTE: it is fine give null as ds framework here, we access datasets V2
                                          //       differently (thru dataset manager that talks to ds service)
                                          null);
    }
  }

  public DataSetSpecification getDataSetSpecification(String name, OperationContext context)
    throws DataSetInstantiationException {
    // get the data set spec from the meta data store
    String jsonSpec = null;
    try {
      DataSetSpecification spec = store.getDataSet(new Id.Account(context.getAccount()), name);
      String json =  spec == null ? "" : GSON.toJson(makeDataSetRecord(spec.getName(), spec.getType(), spec));
      if (json != null) {
        Map<String, String> map = GSON.fromJson(json, new TypeToken<Map<String, String>>() { }.getType());
        if (map != null) {
          jsonSpec = map.get("specification");
        }
      }
      if (jsonSpec == null || jsonSpec.isEmpty()) {
        throw new DataSetInstantiationException(
          "Data set '" + name + "' has no specification in meta data service.");
      }
      return GSON.fromJson(jsonSpec, DataSetSpecification.class);

    } catch (JsonSyntaxException e) {
      throw new DataSetInstantiationException(
        "Error deserializing data set spec for '" + name + "' from JSON in meta data service.", e);
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw new DataSetInstantiationException ("Could not retrieve data specs for " +
                             context.getAccount() + ", reason: " + e.getMessage());
    }
  }


  public void createDataSet(String accountId, String spec) throws Exception {
    try {
      DataSetSpecification streamSpec = GSON.fromJson(spec, DataSetSpecification.class);
      store.addDataset(new Id.Account(accountId), streamSpec);
    } catch (OperationException e) {
      LOG.warn(e.getMessage(), e);
      throw  new Exception("Could not create dataset for " +
                                             accountId + ", reason: " + e.getMessage());
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      throw new Exception(throwable.getMessage());
    }
  }

  // used only for unit-tests
  public DataSetInstantiationBase getInstantiator() {
    return instantiator;
  }

  private static Map<String, String> makeDataSetRecord(String name, String classname,
                                                       DataSetSpecification specification) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("type", "Dataset");
    builder.put("id", name);
    builder.put("name", name);
    if (classname != null) {
      builder.put("classname", classname);
    }
    if (specification != null) {
      builder.put("specification", GSON.toJson(specification));
    }
    return builder.build();
  }

}
