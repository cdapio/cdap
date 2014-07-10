package com.continuuity.data2.dataset2.lib.table.inmemory;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDatasetDefinition;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableClient;

import java.io.IOException;

/**
 *
 */
public class InMemoryOrderedTableDefinition
  extends AbstractDatasetDefinition<OrderedTable, InMemoryOrderedTableAdmin> {

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
  public OrderedTable getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    ConflictDetection conflictDetection =
      ConflictDetection.valueOf(spec.getProperty("conflict.level", ConflictDetection.ROW.name()));
    return new InMemoryOcTableClient(spec.getName(), conflictDetection);
  }

  @Override
  public InMemoryOrderedTableAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    // todo: or pass the full spec?
    return new InMemoryOrderedTableAdmin(spec.getName());
  }
}
