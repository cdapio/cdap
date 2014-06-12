package com.continuuity.data2.dataset2.lib.table.leveldb;

import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.data2.dataset2.lib.table.ConflictDetection;
import com.continuuity.internal.data.dataset.DatasetProperties;
import com.continuuity.internal.data.dataset.DatasetSpecification;
import com.google.inject.Inject;

import java.io.IOException;

/**
 *
 */
public class LevelDBOrderedTableDefinition
  extends AbstractDatasetDefinition<LevelDBOrderedTable, LevelDBOrderedTableAdmin> {

  @Inject
  private LevelDBOcTableService service;

  public LevelDBOrderedTableDefinition(String name) {
    super(name);
  }

  @Override
  public DatasetSpecification configure(String name, DatasetProperties properties) {
    return DatasetSpecification.builder(name, getName())
      .properties(properties.getProperties())
      .build();
  }

  @Override
  public LevelDBOrderedTable getDataset(DatasetSpecification spec) throws IOException {
    ConflictDetection conflictDetection =
      ConflictDetection.valueOf(spec.getProperty("conflict.level", ConflictDetection.ROW.name()));
    return new LevelDBOrderedTable(spec.getName(), service, conflictDetection);
  }

  @Override
  public LevelDBOrderedTableAdmin getAdmin(DatasetSpecification spec) throws IOException {
    return new LevelDBOrderedTableAdmin(spec, service);
  }
}
