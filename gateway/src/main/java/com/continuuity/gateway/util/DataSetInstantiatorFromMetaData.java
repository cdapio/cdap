package com.continuuity.gateway.util;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetInstantiationException;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.DataType;
import com.continuuity.app.services.ProgramId;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.data.DataFabric2Impl;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.dataset.DataSetInstantiationBase;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.apache.thrift.protocol.TProtocol;

import java.util.Map;

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

  // the strategy for discovering app-fabric thrift service
  private EndpointStrategy endpointStrategy;

  @Inject
  public DataSetInstantiatorFromMetaData(LocationFactory locationFactory,
                                         DataSetAccessor dataSetAccessor) {
    // set up the data set instantiator
    this.instantiator = new DataSetInstantiationBase();
    // we don't set the data set specs of the instantiator, instead we will
    // do that on demand every time getDataSet() is called

    this.locationFactory = locationFactory;
    this.dataSetAccessor = dataSetAccessor;
  }

  /**
   * This must be called before the instantiator can be used.
   */
  public void init(EndpointStrategy endpointStrategy) {
    this.endpointStrategy = endpointStrategy;
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
      return this.instantiator.getDataSet(name, new DataFabric2Impl(locationFactory, dataSetAccessor));
    }
  }

  public DataSetSpecification getDataSetSpecification(String name, OperationContext context) {
    // get the data set spec from the meta data store
    String jsonSpec = null;
    try {
      Preconditions.checkNotNull(endpointStrategy, "not initialized - endPointStrategy is null.");
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        String json = client.getDataEntity(new ProgramId(context.getAccount(), "", ""), DataType.DATASET, name);
        if (json != null) {
          Map<String, String> map = new Gson().fromJson(json, new TypeToken<Map<String, String>>() {}.getType());
          if (map != null) {
            jsonSpec = map.get("specification");
          }
        }
      } finally {
        if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
    } catch (Exception e) {
      throw new DataSetInstantiationException(
        "Error reading data set spec for '" + name + "' from meta data service.", e);
    }
    if (jsonSpec == null || jsonSpec.isEmpty()) {
      throw new DataSetInstantiationException(
        "Data set '" + name + "' has no specification in meta data service.");
    }
    try {
      return new Gson().fromJson(jsonSpec, DataSetSpecification.class);
    } catch (JsonSyntaxException e) {
      throw new DataSetInstantiationException(
        "Error deserializing data set spec for '" + name + "' from JSON in meta data service.", e);
    }
  }

  // used only for unit-tests
  public DataSetInstantiationBase getInstantiator() {
    return instantiator;
  }
}
