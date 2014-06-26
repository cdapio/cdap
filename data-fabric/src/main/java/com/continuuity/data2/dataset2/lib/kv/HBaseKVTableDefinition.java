package com.continuuity.data2.dataset2.lib.kv;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDatasetDefinition;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Simple implementation of in-memory non-tx {@link NoTxKeyValueTable}.
 */
public class HBaseKVTableDefinition extends AbstractDatasetDefinition<NoTxKeyValueTable, DatasetAdmin> {
  private static final byte[] DATA_COLUMN_FAMILY = Bytes.toBytes("d");

  @Inject
  private Configuration hConf;
  @Inject
  private HBaseTableUtil tableUtil;

  public HBaseKVTableDefinition(String name) {
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
    return new DatasetAdminImpl(spec.getName(), tableUtil, hConf);
  }

  @Override
  public NoTxKeyValueTable getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new KVTableImpl(spec.getName(), hConf);
  }

  private static final class DatasetAdminImpl implements DatasetAdmin {
    private final String tableName;
    protected final HBaseAdmin admin;
    protected final HBaseTableUtil tableUtil;

    private DatasetAdminImpl(String tableName, HBaseTableUtil tableUtil, Configuration hConf) throws IOException {
      this.tableName = tableName;
      this.admin = new HBaseAdmin(hConf);
      this.tableUtil = tableUtil;
    }

    @Override
    public boolean exists() throws IOException {
      return admin.tableExists(tableName);
    }

    @Override
    public void create() throws IOException {
      HColumnDescriptor columnDescriptor = new HColumnDescriptor(DATA_COLUMN_FAMILY);
      // todo: make stuff configurable
      // NOTE: we cannot limit number of versions as there's no hard limit on # of excluded from read txs
      columnDescriptor.setMaxVersions(Integer.MAX_VALUE);
      tableUtil.setBloomFilter(columnDescriptor, HBaseTableUtil.BloomType.ROW);

      HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
      tableDescriptor.addFamily(columnDescriptor);
      tableUtil.createTableIfNotExists(admin, tableName, tableDescriptor);
    }

    @Override
    public void drop() throws IOException {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }

    @Override
    public void truncate() throws IOException {
      byte[] tableName = Bytes.toBytes(this.tableName);
      HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
      admin.createTable(tableDescriptor);
    }

    @Override
    public void upgrade() throws IOException {
      // no-op
    }

    @Override
    public void close() throws IOException {
      admin.close();
    }
  }

  private static final class KVTableImpl implements NoTxKeyValueTable {
    private static final byte[] DEFAULT_COLUMN = Bytes.toBytes("c");

    private final HTable table;

    public KVTableImpl(String tableName, Configuration hConf) throws IOException {
      this.table = new HTable(hConf, tableName);
    }

    @Override
    public void put(byte[] key, @Nullable byte[] value) {
      try {
        if (value == null) {
          table.delete(new Delete(key));
        } else {
          Put put = new Put(key);
          put.add(DATA_COLUMN_FAMILY, DEFAULT_COLUMN, value);
          table.put(put);
        }
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Nullable
    @Override
    public byte[] get(byte[] key) {
      try {
        Result result = table.get(new Get(key));
        return result.isEmpty() ? null : result.getValue(DATA_COLUMN_FAMILY, DEFAULT_COLUMN);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void close() throws IOException {
      table.close();
    }
  }

  /**
   * Registers this type as implementation for {@link NoTxKeyValueTable} using class name.
   */
  public static final class Module implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      registry.add(new HBaseKVTableDefinition(NoTxKeyValueTable.class.getName()));
    }
  }

}
