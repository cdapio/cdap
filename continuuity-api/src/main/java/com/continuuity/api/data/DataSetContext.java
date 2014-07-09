package com.continuuity.api.data;

import java.io.Closeable;

/**
 * A method that instantiates a Dataset at runtime.
 */
public interface DataSetContext {
  /**
   * Get an instance of the specified Dataset.
   *
   * @param name The name of the Dataset
   * @param <T> The type of the Dataset
   * @return A new instance of the specified Dataset
   * @throws DataSetInstantiationException If the Dataset cannot be instantiated: its class
   *         cannot be loaded; the default constructor throws an exception; or the Dataset
   *         cannot be opened (for example, one of the underlying tables in the DataFabric
   *         cannot be accessed).
   */
  public <T extends Closeable> T getDataSet(String name)
      throws DataSetInstantiationException;
}
