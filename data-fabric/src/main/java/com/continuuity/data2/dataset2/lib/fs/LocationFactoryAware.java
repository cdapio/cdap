package com.continuuity.data2.dataset2.lib.fs;

import org.apache.twill.filesystem.LocationFactory;

/**
 * Implementation of this interface needs to access filesystem thru {@link LocationFactory}
 */
public interface LocationFactoryAware {
  /**
   * Sets {@link LocationFactory}
   * @param locationFactory {@link LocationFactory} to set
   */
  void setLocationFactory(LocationFactory locationFactory);
}
