package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.data2.dataset.api.DataSetManager;

import java.util.Properties;

/**
 *
 */
public class
  InMemoryOcTableManager implements DataSetManager {
  @Override
  public boolean exists(String name) {
    return InMemoryOcTableService.exists(name);
  }

  @Override
  public void create(String name) {
    InMemoryOcTableService.create(name);
  }

  @Override
  public void create(String name, @SuppressWarnings("unused") Properties props) throws Exception {
    create(name);
  }

  @Override
  public void truncate(String name) {
    InMemoryOcTableService.truncate(name);
  }

  @Override
  public void drop(String name) {
    InMemoryOcTableService.drop(name);
  }

  @Override
  public void upgrade(String name, Properties properties) throws Exception {
    // No-op
  }
}
