package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.continuuity.internal.data.dataset.lib.table.Table;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.TypeRepresentation;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;

import java.io.IOException;

/**
 * DatasetDefinition for {@link com.continuuity.data2.dataset2.lib.table.MultiObjectStore}.
 *
 * @param <T> Type of object that the {@link com.continuuity.data2.dataset2.lib.table.MultiObjectStore} will store.
 */
public class MultiObjectStoreDefinition<T>
  extends AbstractDatasetDefinition<MultiObjectStore<T>, DatasetAdmin> {

  private static final Gson GSON = new Gson();

  private final DatasetDefinition<? extends Table, ?> tableDef;

  public MultiObjectStoreDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name);
    Preconditions.checkArgument(tableDef != null, "Table definition is required");
    this.tableDef = tableDef;
  }

  @Override
  public DatasetInstanceSpec configure(String instanceName, DatasetInstanceProperties properties) {
    Preconditions.checkArgument(properties.getProperties().containsKey("type"));
    Preconditions.checkArgument(properties.getProperties().containsKey("schema"));
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
  public MultiObjectStore<T> getDataset(DatasetInstanceSpec spec) throws IOException {
    DatasetInstanceSpec tableSpec = spec.getSpecification("table");
    Table table = tableDef.getDataset(tableSpec);

    TypeRepresentation typeRep = GSON.fromJson(spec.getProperty("type"), TypeRepresentation.class);
    Schema schema = GSON.fromJson(spec.getProperty("schema"), Schema.class);
    return new MultiObjectStore<T>(spec.getName(), table, typeRep, schema);
  }

}
