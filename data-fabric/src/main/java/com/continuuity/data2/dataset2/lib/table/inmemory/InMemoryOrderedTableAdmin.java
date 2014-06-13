package com.continuuity.data2.dataset2.lib.table.inmemory;

import com.continuuity.api.dataset.DatasetAdmin;

import java.io.IOException;

/**
 *
 */
public class InMemoryOrderedTableAdmin implements DatasetAdmin {
  private final String name;

  public InMemoryOrderedTableAdmin(String name) {
    this.name = name;
  }

  @Override
  public boolean exists() {
    return InMemoryOrderedTableService.exists(name);
  }

  @Override
  public void create() {
    InMemoryOrderedTableService.create(name);
  }

  @Override
  public void truncate() {
    InMemoryOrderedTableService.truncate(name);
  }

  @Override
  public void drop() {
    InMemoryOrderedTableService.drop(name);
  }

  @Override
  public void upgrade() {
    // no-op
  }

  @Override
  public void close() throws IOException {
    // NOTHING to do
  }
}
