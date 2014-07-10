package com.continuuity.data2.dataset2.lib.table.leveldb;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDatasetDefinition;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableClient;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.google.inject.Inject;

import java.io.IOException;

/**
 *
 */
public class LevelDBOrderedTableDefinition
  extends AbstractDatasetDefinition<OrderedTable, LevelDBOrderedTableAdmin> {

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
  public OrderedTable getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    ConflictDetection level =
      ConflictDetection.valueOf(spec.getProperty("conflict.level", ConflictDetection.ROW.name()));
    return new LevelDBOcTableClient(spec.getName(), level, service);
  }

  @Override
  public LevelDBOrderedTableAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new LevelDBOrderedTableAdmin(spec, service);
  }
}
