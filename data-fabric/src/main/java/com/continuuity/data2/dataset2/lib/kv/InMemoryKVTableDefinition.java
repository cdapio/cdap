package com.continuuity.data2.dataset2.lib.kv;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDatasetDefinition;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * Simple implementation of in-memory non-tx {@link NoTxKeyValueTable}.
 */
public class InMemoryKVTableDefinition extends AbstractDatasetDefinition<NoTxKeyValueTable, DatasetAdmin> {
  private static final Map<String, Map<byte[], byte[]>> tables = Maps.newHashMap();

  public InMemoryKVTableDefinition(String name) {
    super(name);
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new DatasetAdminImpl(spec.getName());
  }

  @Override
  public NoTxKeyValueTable getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new InMemoryKVTable(spec.getName());
  }

  private static final class DatasetAdminImpl implements DatasetAdmin {
    private final String tableName;

    private DatasetAdminImpl(String tableName) {
      this.tableName = tableName;
    }

    @Override
    public boolean exists() throws IOException {
      return tables.containsKey(tableName);
    }

    @Override
    public void create() throws IOException {
      tables.put(tableName, new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR));
    }

    @Override
    public void drop() throws IOException {
      tables.remove(tableName);
    }

    @Override
    public void truncate() throws IOException {
      create();
    }

    @Override
    public void upgrade() throws IOException {
      // no-op
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }

  private static final class InMemoryKVTable implements NoTxKeyValueTable {
    private final String tableName;

    private Map<byte[], byte[]> getTable() {
      return tables.get(tableName);
    }

    public InMemoryKVTable(String tableName) {
      Preconditions.checkArgument(tables.containsKey(tableName), "Table does not exist: " + tableName);
      this.tableName = tableName;
    }

    @Override
    public void put(byte[] key, @Nullable byte[] value) {
      if (value == null) {
        getTable().remove(key);
      } else {
        getTable().put(key, value);
      }
    }

    @Nullable
    @Override
    public byte[] get(byte[] key) {
      return getTable().get(key);
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }

  /**
   * Registers this type as implementation for {@link NoTxKeyValueTable} using class name.
   */
  public static final class Module implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      registry.add(new InMemoryKVTableDefinition(NoTxKeyValueTable.class.getName()));
    }
  }

}
