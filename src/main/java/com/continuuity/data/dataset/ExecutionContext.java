package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetInstantiationException;
import com.continuuity.api.data.OperationException;

/**
 * this is to illustrate how, e.g., a flow context can instantiate a
 * data set.
 */
public interface ExecutionContext {
  public <T extends DataSet> T getDataSet(String name)
      throws DataSetInstantiationException, OperationException;
}
