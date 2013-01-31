package com.continuuity.data;

/**
 * Interface for registering DataLibs.
 */
public interface DataSetRegistry {

  /**
   * Registers a DataLib.
   *
   * @param dataLib To be registered for dataset.
   * @return Registered DataLib.
   */
  public DataLib registerDataSet(DataLib dataLib);
}
