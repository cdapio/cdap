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
 * DatasetDefinition for {@link com.continuuity.data2.dataset2.lib.table.ObjectStore}.
 *
 * @param <T> Type of object that the {@link com.continuuity.data2.dataset2.lib.table.ObjectStore} will store.
 */
public class IndexedObjectStoreDefinition<T>
  extends AbstractDatasetDefinition<IndexedObjectStore<T>, DatasetAdmin> {

  private final DatasetDefinition<? extends Table, ?> tableDef;
  private final DatasetDefinition<? extends ObjectStore, ?> objectStoreDef;

  public IndexedObjectStoreDefinition(String name,
                                      DatasetDefinition<? extends Table, ?> tableDef,
                                      DatasetDefinition<? extends ObjectStore, ?> objectStoreDef) {
    super(name);
    Preconditions.checkArgument(tableDef != null, "Table definition is required");
    Preconditions.checkArgument(objectStoreDef != null, "ObjectStore definition is required");
    this.tableDef = tableDef;
    this.objectStoreDef = objectStoreDef;
  }

  @Override
  public DatasetInstanceSpec configure(String instanceName, DatasetInstanceProperties properties) {
    return new DatasetInstanceSpec.Builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("table", properties.getProperties("table")),
                objectStoreDef.configure("objectStore", properties.getProperties("objectStore")))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetInstanceSpec spec) throws IOException {
    return new CompositeDatasetAdmin(Lists.newArrayList(
      tableDef.getAdmin(spec.getSpecification("table")),
      objectStoreDef.getAdmin(spec.getSpecification("objectStore"))
    ));
  }

  @Override
  public IndexedObjectStore<T> getDataset(DatasetInstanceSpec spec) throws IOException {
    DatasetInstanceSpec tableSpec = spec.getSpecification("table");
    DatasetInstanceSpec objectStoreSpec = spec.getSpecification("objectStore");

    Table index = tableDef.getDataset(tableSpec);
    ObjectStore<T> objectStore = objectStoreDef.getDataset(objectStoreSpec);

    return new IndexedObjectStore<T>(spec.getName(), objectStore, index);
  }

}
