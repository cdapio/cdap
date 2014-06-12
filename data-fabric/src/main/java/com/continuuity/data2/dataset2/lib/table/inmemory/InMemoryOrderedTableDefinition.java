package com.continuuity.data2.dataset2.lib.table.inmemory;

import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.data2.dataset2.lib.table.ConflictDetection;
import com.continuuity.internal.data.dataset.DatasetProperties;
import com.continuuity.internal.data.dataset.DatasetSpecification;

import java.io.IOException;

/**
 *
 */
public class InMemoryOrderedTableDefinition
  extends AbstractDatasetDefinition<InMemoryOrderedTable, InMemoryOrderedTableAdmin> {

  public InMemoryOrderedTableDefinition(String name) {
    super(name);
  }

  @Override
  public DatasetSpecification configure(String name, DatasetProperties properties) {
    return DatasetSpecification.builder(name, getName())
      .properties(properties.getProperties())
      .build();
  }

  @Override
  public InMemoryOrderedTable getDataset(DatasetSpecification spec) throws IOException {
    ConflictDetection conflictDetection =
      ConflictDetection.valueOf(spec.getProperty("conflict.level", ConflictDetection.ROW.name()));
    return new InMemoryOrderedTable(spec.getName(), conflictDetection);
  }

  @Override
  public InMemoryOrderedTableAdmin getAdmin(DatasetSpecification spec) throws IOException {
    // todo: or pass the full spec?
    return new InMemoryOrderedTableAdmin(spec.getName());
  }
}
