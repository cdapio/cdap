package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSet;

/**
 * This interface defines a single method to instantiate a data set at runtime.
 */
public interface DataSetContext {
  /**
   * Get an instance of the named data set.
   * @param name the name of the data set
   * @param <T> the type of the data set
   * @return a new instance of the named data set
   * @throws DataSetInstantiationException if for any reason, the data set
   *         cannot be instantiated, for instance, its class cannot be loaded,
   *         its class is missing the runtime constructor (@see
   *         Dataset#DataSet(DataSetSpecification)), or the constructor throws
   *         an exception. Also if a data set cannot be opened, for instance,
   *         if we fail to access one of the underlying Tables in the data
   *         fabric.
   */
  public <T extends DataSet> T getDataSet(String name)
      throws DataSetInstantiationException;
}
