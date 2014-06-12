package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.data2.dataset2.lib.CompositeDatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetProperties;
import com.continuuity.internal.data.dataset.DatasetSpecification;
import com.continuuity.internal.data.dataset.lib.table.Table;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("table", properties),
                objectStoreDef.configure("objectStore", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec) throws IOException {
    return new CompositeDatasetAdmin(Lists.newArrayList(
      tableDef.getAdmin(spec.getSpecification("table")),
      objectStoreDef.getAdmin(spec.getSpecification("objectStore"))
    ));
  }

  @Override
  public IndexedObjectStore<T> getDataset(DatasetSpecification spec) throws IOException {
    DatasetSpecification tableSpec = spec.getSpecification("table");
    DatasetSpecification objectStoreSpec = spec.getSpecification("objectStore");

    Table index = tableDef.getDataset(tableSpec);
    ObjectStore<T> objectStore = objectStoreDef.getDataset(objectStoreSpec);

    return new IndexedObjectStore<T>(spec.getName(), objectStore, index);
  }

}
