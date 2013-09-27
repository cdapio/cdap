package com.continuuity.gateway;

import com.continuuity.metadata.MetaDataStore;

/**
 * Defines the interface for the class that uses {@link com.continuuity.metadata.MetaDataStore}.
 */
public interface MetaDataServiceAware {
  /**
   * Set the meta data service.
   *
   * @param service the metadata servrice to set
   */
  void setMetadataService(MetaDataStore service);
}
