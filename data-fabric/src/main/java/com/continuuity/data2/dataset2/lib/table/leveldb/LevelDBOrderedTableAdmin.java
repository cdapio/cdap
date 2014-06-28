package com.continuuity.data2.dataset2.lib.table.leveldb;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;

import java.io.IOException;

/**
 *
 */
public class LevelDBOrderedTableAdmin implements DatasetAdmin {

  private final LevelDBOcTableService service;
  private final String name;

  public LevelDBOrderedTableAdmin(DatasetSpecification spec, LevelDBOcTableService service) throws IOException {
    this.service = service;
    this.name = spec.getName();
  }

  @Override
  public boolean exists() throws IOException {
    try {
      service.getTable(name);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void create() throws IOException {
    service.ensureTableExists(name);
  }

  @Override
  public void drop() throws IOException {
    service.dropTable(name);
  }

  @Override
  public void truncate() throws IOException {
    drop();
    create();
  }

  @Override
  public void upgrade() throws IOException {
    // no-op
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
