package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.continuuity.internal.io.UnsupportedTypeException;

import java.io.IOException;

/**
 *
 */
public class IntegerStoreDefinition
  extends AbstractDatasetDefinition<ObjectStore<Integer>, DatasetAdmin> {

  private final DatasetDefinition<? extends KeyValueTable, ?> tableDef;

  public IntegerStoreDefinition(String name, DatasetDefinition<? extends KeyValueTable, ?> keyValueTableDefinition) {
    super(name);
    this.tableDef = keyValueTableDefinition;
  }

  @Override
  public DatasetInstanceSpec configure(String instanceName, DatasetInstanceProperties properties) {
    return new DatasetInstanceSpec.Builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("table", properties.getProperties("table")))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetInstanceSpec spec) throws IOException {
    return tableDef.getAdmin(spec.getSpecification("table"));
  }

  @Override
  public ObjectStore<Integer> getDataset(DatasetInstanceSpec spec) throws IOException {
    DatasetInstanceSpec kvTableSpec = spec.getSpecification("table");
    KeyValueTable table = tableDef.getDataset(kvTableSpec);

    try {
      return new IntegerStore(spec.getName(), table);
    } catch (UnsupportedTypeException e) {
      // shouldn't happen
      throw new IOException(e);
    }
  }

}
