package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.TypeRepresentation;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;

import java.io.IOException;

/**
 * DatasetDefinition for {@link ObjectStore}.
 *
 * @param <T> Type of object that the {@link ObjectStore} will store.
 */
public class ObjectStoreDefinition<T>
  extends AbstractDatasetDefinition<ObjectStore<T>, DatasetAdmin> {

  private static final Gson GSON = new Gson();

  private final DatasetDefinition<? extends KeyValueTable, ?> tableDef;

  public ObjectStoreDefinition(String name, DatasetDefinition<? extends KeyValueTable, ?> keyValueTableDefinition) {
    super(name);
    this.tableDef = keyValueTableDefinition;
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
  public ObjectStore<T> getDataset(DatasetInstanceSpec spec) throws IOException {
    DatasetInstanceSpec kvTableSpec = spec.getSpecification("table");
    KeyValueTable table = tableDef.getDataset(kvTableSpec);

    TypeRepresentation typeRep = GSON.fromJson(spec.getProperty("type"), TypeRepresentation.class);
    Schema schema = GSON.fromJson(spec.getProperty("schema"), Schema.class);
    return new ObjectStore<T>(spec.getName(), table, typeRep, schema);
  }

}
