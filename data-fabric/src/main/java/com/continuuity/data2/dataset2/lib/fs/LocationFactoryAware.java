package com.continuuity.data2.dataset2.lib.fs;

import org.apache.twill.filesystem.LocationFactory;

/**
 *
 */
public interface LocationFactoryAware {
  void setLocationFactory(LocationFactory locationFactory);
}
