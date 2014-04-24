package com.continuuity.data2.dataset2.lib.table.inmemory;

import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.continuuity.data2.dataset2.lib.table.ConflictDetection;

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
  public DatasetInstanceSpec configure(String name, DatasetInstanceProperties properties) {
    return new DatasetInstanceSpec.Builder(name, getName())
      .properties(properties.getProperties())
      .build();
  }

  @Override
  public InMemoryOrderedTable getDataset(DatasetInstanceSpec spec) throws IOException {
    ConflictDetection conflictDetection =
      ConflictDetection.valueOf(spec.getProperty("conflict.level", ConflictDetection.ROW.name()));
    return new InMemoryOrderedTable(spec.getName(), conflictDetection);
  }

  @Override
  public InMemoryOrderedTableAdmin getAdmin(DatasetInstanceSpec spec) throws IOException {
    // todo: or pass the full spec?
    return new InMemoryOrderedTableAdmin(spec.getName());
  }
}
