package com.continuuity.data2.dataset2.lib.table.leveldb;

import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.data2.dataset2.lib.leveldb.LevelDBAware;
import com.continuuity.data2.dataset2.lib.table.ConflictDetection;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;

import java.io.IOException;

/**
 *
 */
public class LevelDBOrderedTableDefinition
  extends AbstractDatasetDefinition<LevelDBOrderedTable, LevelDBOrderedTableAdmin> implements LevelDBAware {

  private LevelDBOcTableService service;

  public LevelDBOrderedTableDefinition(String name) {
    super(name);
  }

  @Override
  public void setLevelDBService(LevelDBOcTableService service) {
    this.service = service;
  }

  @Override
  public DatasetInstanceSpec configure(String name, DatasetInstanceProperties properties) {
    return new DatasetInstanceSpec.Builder(name, getName())
      .properties(properties.getProperties())
      .build();
  }

  @Override
  public LevelDBOrderedTable getDataset(DatasetInstanceSpec spec) throws IOException {
    ConflictDetection conflictDetection =
      ConflictDetection.valueOf(spec.getProperty("conflict.level", ConflictDetection.ROW.name()));
    return new LevelDBOrderedTable(spec.getName(), service, conflictDetection);
  }

  @Override
  public LevelDBOrderedTableAdmin getAdmin(DatasetInstanceSpec spec) throws IOException {
    return new LevelDBOrderedTableAdmin(spec, service);
  }
}
