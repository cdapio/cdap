package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDatasetDefinition;
import com.continuuity.api.dataset.lib.KeyValueTable;
import com.continuuity.api.dataset.lib.ObjectStore;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaTypeAdapter;
import com.continuuity.internal.io.TypeRepresentation;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;

/**
 * DatasetDefinition for {@link ObjectStoreDataset}.
 */
public class ObjectStoreDefinition
  extends AbstractDatasetDefinition<ObjectStore, DatasetAdmin> {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private final DatasetDefinition<? extends KeyValueTable, ?> tableDef;

  public ObjectStoreDefinition(String name, DatasetDefinition<? extends KeyValueTable, ?> keyValueDef) {
    super(name);
    Preconditions.checkArgument(keyValueDef != null, "KeyValueTable definition is required");
    this.tableDef = keyValueDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    Preconditions.checkArgument(properties.getProperties().containsKey("type"));
    Preconditions.checkArgument(properties.getProperties().containsKey("schema"));
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("objects", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return tableDef.getAdmin(spec.getSpecification("objects"), classLoader);
  }

  @Override
  public ObjectStoreDataset<?> getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    DatasetSpecification kvTableSpec = spec.getSpecification("objects");
    KeyValueTable table = tableDef.getDataset(kvTableSpec, classLoader);

    TypeRepresentation typeRep = GSON.fromJson(spec.getProperty("type"), TypeRepresentation.class);
    Schema schema = GSON.fromJson(spec.getProperty("schema"), Schema.class);
    return new ObjectStoreDataset(spec.getName(), table, typeRep, schema, classLoader);
  }

}
