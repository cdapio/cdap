package com.continuuity.api.dataset.lib;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.table.Table;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;

/**
 * DatasetDefinition for {@link IndexedTable}.
 */
public class IndexedTableDefinition
  extends AbstractDatasetDefinition<IndexedTable, DatasetAdmin> {
  
  private final DatasetDefinition<? extends Table, ?> tableDef;

  public IndexedTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name);
    Preconditions.checkArgument(tableDef != null, "Table definition is required");
    this.tableDef = tableDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("d", properties),
                tableDef.configure("i", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec) throws IOException {
    return new CompositeDatasetAdmin(Lists.newArrayList(
      tableDef.getAdmin(spec.getSpecification("d")),
      tableDef.getAdmin(spec.getSpecification("i"))
    ));
  }

  @Override
  public IndexedTable getDataset(DatasetSpecification spec) throws IOException {
    DatasetSpecification tableInstance = spec.getSpecification("d");
    Table table = tableDef.getDataset(tableInstance);

    DatasetSpecification indexTableInstance = spec.getSpecification("i");
    Table index = tableDef.getDataset(indexTableInstance);

    String columnToIndex = spec.getProperty("columnToIndex");
    Preconditions.checkNotNull(columnToIndex, "columnToIndex must be specified");
//    int ttl = spec.getIntProperty("ttl", -1);

    return new IndexedTable(spec.getName(), table, index, columnToIndex);
  }

}
