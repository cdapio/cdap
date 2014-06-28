package com.continuuity.data2.datafabric.dataset.service.mds;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.module.EmbeddedDataset;
import com.continuuity.api.dataset.table.OrderedTable;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Dataset instances metadata store
 */
public final class DatasetInstanceMDS extends AbstractObjectsStore {
  /**
   * Prefix for rows containing instance info.
   * NOTE: even though we don't have to have it now we may want to store different type of data in one table, so
   *       the prefix may help us in future
   */
  private static final byte[] INSTANCE_PREFIX = Bytes.toBytes("i_");

  public DatasetInstanceMDS(DatasetSpecification spec, @EmbeddedDataset("") OrderedTable table) {
    super(spec, table);
  }

  @Nullable
  public DatasetSpecification get(String name) {
    return get(getInstanceKey(name), DatasetSpecification.class);
  }

  public void write(DatasetSpecification instanceSpec) {
    put(getInstanceKey(instanceSpec.getName()), instanceSpec);
  }

  public boolean delete(String name) {
    if (get(name) == null) {
      return false;
    }
    delete(getInstanceKey(name));
    return true;
  }

  public Collection<DatasetSpecification> getAll() {
    Map<String, DatasetSpecification> instances = scan(INSTANCE_PREFIX, DatasetSpecification.class);
    return instances.values();
  }

  public Collection<DatasetSpecification> getByTypes(Set<String> typeNames) {
    List<DatasetSpecification> filtered = Lists.newArrayList();

    for (DatasetSpecification spec : getAll()) {
      if (typeNames.contains(spec.getType())) {
        filtered.add(spec);
      }
    }

    return filtered;
  }

  public void deleteAll() {
    deleteAll(INSTANCE_PREFIX);
  }

  private byte[] getInstanceKey(String name) {
    return Bytes.add(INSTANCE_PREFIX, Bytes.toBytes(name));
  }
}
