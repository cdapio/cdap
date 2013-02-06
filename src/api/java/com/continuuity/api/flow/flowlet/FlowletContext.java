package com.continuuity.api.flow.flowlet;

import com.continuuity.api.data.DataSet;

/**
 * This interface represents the Flowlet context, which consists of things like
 * number of instances of this flowlet and methods to create and initialize a
 * {@link DataSet} by name.
 */
public interface FlowletContext {

  /**
   * @return Number of instances of this flowlet.
   */
  int getInstanceCount();

  /**
   * Returns a reference to {@link DataSet} instance based on the name specified.
   * @param name of the dataset
   * @return null if not found; otherwise the {@link DataSet} instance.
   */
  DataSet getDataSet(String name);
}
