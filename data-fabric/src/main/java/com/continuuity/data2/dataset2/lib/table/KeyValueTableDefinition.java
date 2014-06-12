package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetProperties;
import com.continuuity.internal.data.dataset.DatasetSpecification;
import com.continuuity.internal.data.dataset.lib.table.Table;
import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * {@link DatasetDefinition} for {@link KeyValueTable}.
 */
public class KeyValueTableDefinition
  extends AbstractDatasetDefinition<KeyValueTable, DatasetAdmin> {

  private final DatasetDefinition<? extends Table, ?> tableDef;

  public KeyValueTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name);
    Preconditions.checkArgument(tableDef != null, "Table definition is required");
    this.tableDef = tableDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("table", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec) throws IOException {
    return tableDef.getAdmin(spec.getSpecification("table"));
  }

  @Override
  public KeyValueTable getDataset(DatasetSpecification spec) throws IOException {
    Table table = tableDef.getDataset(spec.getSpecification("table"));
    return new KeyValueTable(spec.getName(), table);
  }
}
