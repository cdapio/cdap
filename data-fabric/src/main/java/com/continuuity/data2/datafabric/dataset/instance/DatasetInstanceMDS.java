package com.continuuity.data2.datafabric.dataset.instance;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.datafabric.dataset.AbstractObjectsStore;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;

/**
 * Dataset instances metadata store
 */
final class DatasetInstanceMDS extends AbstractObjectsStore {
  /**
   * Prefix for rows containing instance info.
   * NOTE: even though we don't have to have it now we may want to store different type of data in one table, so
   *       the prefix may help us in future
   */
  private static final byte[] INSTANCE_PREFIX = Bytes.toBytes("i_");

  public DatasetInstanceMDS(OrderedTable table) {
    super(table);
  }

  @Nullable
  public DatasetInstanceSpec get(String name) {
    return get(getInstanceKey(name), DatasetInstanceSpec.class);
  }

  @Nullable
  public void write(DatasetInstanceSpec instanceSpec) {
    put(getInstanceKey(instanceSpec.getName()), instanceSpec);
  }

  public boolean delete(String name) {
    if (get(name) == null) {
      return false;
    }
    delete(getInstanceKey(name));
    return true;
  }

  public Collection<DatasetInstanceSpec> getAll() {
    Map<String, DatasetInstanceSpec> instances = scan(INSTANCE_PREFIX, DatasetInstanceSpec.class);
    return instances.values();
  }

  private byte[] getInstanceKey(String name) {
    return Bytes.add(INSTANCE_PREFIX, Bytes.toBytes(name));
  }
}
