package com.continuuity.gateway;

import com.continuuity.data.metadata.MetaDataStore;

/**
 * Defines the interface for the class that uses {@link com.continuuity.data.metadata.MetaDataStore}.
 */
public interface MetaDataStoreAware {
  /**
   * Set the meta data store.
   *
   * @param mds the metadata store to set
   */
  void setMetadataStore(MetaDataStore mds);
}
