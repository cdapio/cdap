package com.continuuity.gateway;

import com.continuuity.app.store.Store;

/**
 * Defines the interface for the class that uses {@link com.continuuity.app.store.Store}.
 */
public interface StoreAware {
  /**
   * Set the datastore.
   *
   * @param store the store to set
   */
  void setStore(Store store);
}
