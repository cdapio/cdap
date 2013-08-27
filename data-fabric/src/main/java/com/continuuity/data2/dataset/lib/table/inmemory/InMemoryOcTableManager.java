package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.data2.dataset.api.DataSetManager;

/**
 *
 */
public class InMemoryOcTableManager implements DataSetManager {
  @Override
  public boolean exists(String name) {
    return InMemoryOcTableService.exists(name);
  }

  @Override
  public void create(String name) {
    InMemoryOcTableService.create(name);
  }

  @Override
  public void truncate(String name) {
    InMemoryOcTableService.truncate(name);
  }

  @Override
  public void drop(String name) {
    InMemoryOcTableService.drop(name);
  }
}
