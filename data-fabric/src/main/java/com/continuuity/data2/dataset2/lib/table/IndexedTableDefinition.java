package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.data2.dataset2.lib.CompositeDatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.continuuity.internal.data.dataset.lib.table.Table;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.TypeRepresentation;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.io.IOException;

/**
 * DatasetDefinition for {@link MultiObjectStore}.
 *
 * @param <T> Type of object that the {@link MultiObjectStore} will store.
 */
public class IndexedTableDefinition<T>
  extends AbstractDatasetDefinition<IndexedTable, DatasetAdmin> {
  
  private final DatasetDefinition<? extends Table, ?> tableDef;

  public IndexedTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name);
    Preconditions.checkArgument(tableDef != null, "Table definition is required");
    this.tableDef = tableDef;
  }

  @Override
  public DatasetInstanceSpec configure(String instanceName, DatasetInstanceProperties properties) {
    return new DatasetInstanceSpec.Builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("table", properties.getProperties("table")),
                tableDef.configure("index", properties.getProperties("index")))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetInstanceSpec spec) throws IOException {
    return new CompositeDatasetAdmin(Lists.newArrayList(
      tableDef.getAdmin(spec.getSpecification("table")),
      tableDef.getAdmin(spec.getSpecification("index"))
    ));
  }

  @Override
  public IndexedTable getDataset(DatasetInstanceSpec spec) throws IOException {
    DatasetInstanceSpec tableInstance = spec.getSpecification("table");
    Table table = tableDef.getDataset(tableInstance);

    DatasetInstanceSpec indexTableInstance = spec.getSpecification("index");
    Table index = tableDef.getDataset(indexTableInstance);

    String columnToIndex = spec.getRequiredProperty("columnToIndex");
//    int ttl = spec.getIntProperty("ttl", -1);

    return new IndexedTable(spec.getName(), table, index, columnToIndex);
  }

}
