package com.continuuity.data.dataset;

import com.continuuity.api.data.OperationException;

public interface ApplicationContext {
  public <T extends DataSet> T getDataSet(String name) throws DataSetInstantiationException, OperationException;
}
