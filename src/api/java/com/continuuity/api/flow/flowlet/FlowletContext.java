package com.continuuity.api.flow.flowlet;

import com.continuuity.api.data.DataSet;

/**
 *
 */
public interface FlowletContext {

  int getInstanceCount();

  DataSet getDataSet(String name);
}
