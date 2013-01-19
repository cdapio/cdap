package com.continuuity.data.dataset;

import com.continuuity.api.data.BatchCollectionClient;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.OperationException;

import java.util.List;

/**
 * This is sample code to show how an execution context (e.g. a flow context)
 * can use the data set instantiator.
 */
public class ExecutionContextImpl implements ExecutionContext {

  private DataSetInstantiator instantiator = new DataSetInstantiator();

  @Override
  public <T extends DataSet> T getDataSet(String name) throws DataSetInstantiationException, OperationException {
    return this.instantiator.instantiate(name);
  }

  public void setBatchCollectionClient(BatchCollectionClient client) {
    this.instantiator.setBatchCollectionClient(client);
  }

  public void setDataFabric(DataFabric fabric) {
    this.instantiator.setDataFabric(fabric);
  }

  public void setDataSets(List<DataSetSpecification> specs) {
    this.instantiator.setDataSets(specs);
  }
}

