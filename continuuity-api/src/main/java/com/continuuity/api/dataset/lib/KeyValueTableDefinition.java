package com.continuuity.api.dataset.lib;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.table.Table;
import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * {@link DatasetDefinition} for {@link KeyValueTable}.
 */
@Beta
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
      .datasets(tableDef.configure("kv", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return tableDef.getAdmin(spec.getSpecification("kv"), classLoader);
  }

  @Override
  public KeyValueTable getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    Table table = tableDef.getDataset(spec.getSpecification("kv"), classLoader);
    return new KeyValueTable(spec.getName(), table);
  }
}
