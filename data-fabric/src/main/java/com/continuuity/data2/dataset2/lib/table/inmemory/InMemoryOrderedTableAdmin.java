package com.continuuity.data2.dataset2.lib.table.inmemory;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableService;

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
    return InMemoryOcTableService.exists(name);
  }

  @Override
  public void create() {
    InMemoryOcTableService.create(name);
  }

  @Override
  public void truncate() {
    InMemoryOcTableService.truncate(name);
  }

  @Override
  public void drop() {
    InMemoryOcTableService.drop(name);
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
