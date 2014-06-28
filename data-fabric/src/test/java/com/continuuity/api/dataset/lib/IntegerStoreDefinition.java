package com.continuuity.api.dataset.lib;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.data2.dataset2.lib.table.ObjectStoreDataset;
import com.continuuity.internal.io.UnsupportedTypeException;

import java.io.IOException;

/**
 *
 */
public class IntegerStoreDefinition
  extends AbstractDatasetDefinition<ObjectStoreDataset<Integer>, DatasetAdmin> {

  private final DatasetDefinition<? extends KeyValueTable, ?> tableDef;

  public IntegerStoreDefinition(String name, DatasetDefinition<? extends KeyValueTable, ?> keyValueTableDefinition) {
    super(name);
    this.tableDef = keyValueTableDefinition;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("table", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return tableDef.getAdmin(spec.getSpecification("table"), classLoader);
  }

  @Override
  public ObjectStoreDataset<Integer> getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    DatasetSpecification kvTableSpec = spec.getSpecification("table");
    KeyValueTable table = tableDef.getDataset(kvTableSpec, classLoader);

    try {
      return new IntegerStore(spec.getName(), table);
    } catch (UnsupportedTypeException e) {
      // shouldn't happen
      throw new IOException(e);
    }
  }

}
