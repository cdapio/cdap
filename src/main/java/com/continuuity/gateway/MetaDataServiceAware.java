package com.continuuity.gateway;

import com.continuuity.metadata.MetadataService;

/**
 * Defines the interface for the class that uses {@link MetadataService}.
 */
public interface MetaDataServiceAware {
  /**
   * Set the meta data service.
   *
   * @param service the metadata servrice to set
   */
  void setMetadataService(MetadataService service);
}
