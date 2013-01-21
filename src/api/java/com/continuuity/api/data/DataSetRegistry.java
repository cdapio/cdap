package com.continuuity.api.data;

/**
 * Interface for registering DataLibs.
 */
public interface DataSetRegistry {

  /**
   * Registers a DataLib.
   *
   * @param dataLib To be registered for com.continuuity.data.dataset.
   * @return Registered DataLib.
   */
  public DataLib registerDataSet(DataLib dataLib);
}
