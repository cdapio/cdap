package com.continuuity.data2.dataset2.manager.inmemory;

import com.continuuity.data2.dataset2.manager.AbstractDatasetManagerTest;
import com.continuuity.data2.dataset2.manager.DatasetManager;

/**
 *
 */
public class InMemoryDatasetManagerTest extends AbstractDatasetManagerTest {
  @Override
  protected DatasetManager getDatasetManager() {
    return new InMemoryDatasetManager();
  }
}
