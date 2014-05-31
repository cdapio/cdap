package com.continuuity.api.data;

/**
 * A method that instantiates a dataset at runtime.
 */
public interface DataSetContext {
  /**
   * Get an instance of the specified dataset.
   * @param name The name of the dataset.
   * @param <T> The type of the dataset.
   * @return A new instance of the specified dataset.
   * @throws DataSetInstantiationException If for any reason the dataset
   *         cannot be instantiated, for instance its class cannot be loaded,
   *         the default constructor throws an exception, or the dataset cannot be opened (for example,
   *         if one of the underlying tables in the DataFabric cannot be accessed).
   */
  public <T extends DataSet> T getDataSet(String name)
      throws DataSetInstantiationException;
}
