package com.continuuity.api.data;

/**
 * Interface for registering DataLibs.
 */
public interface DataSetRegistry {

  /**
   * Registers a DataLib.
   *
   * @param dataLib To be registered for dataset.
   * @return true if successfully registered; false otherwise.
   */
  public boolean registerDataSet(DataLib dataLib);
}
