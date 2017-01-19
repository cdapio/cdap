/*
 * Copyright © 2014-2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.dataset2.lib.kv;

import co.cask.cdap.api.annotation.ReadOnly;
import co.cask.cdap.api.annotation.WriteOnly;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.IncompatibleUpdateException;
import co.cask.cdap.api.dataset.Reconfigurable;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HTableDescriptorBuilder;
import co.cask.cdap.hbase.ddl.ColumnFamilyDescriptor;
import co.cask.cdap.hbase.ddl.CoprocessorDescriptor;
import co.cask.cdap.hbase.ddl.TableDescriptor;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Simple implementation of hbase non-tx {@link NoTxKeyValueTable}.
 */
public class HBaseKVTableDefinition extends AbstractDatasetDefinition<NoTxKeyValueTable, DatasetAdmin>
  implements Reconfigurable {

  private static final byte[] DATA_COLUMN_FAMILY = Bytes.toBytes("d");

  @Inject
  private CConfiguration cConf;
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
  public DatasetSpecification reconfigure(String name,
                                          DatasetProperties properties,
                                          DatasetSpecification currentSpec) throws IncompatibleUpdateException {
    return DatasetSpecification.builder(name, getName())
      .properties(properties.getProperties())
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                               ClassLoader classLoader) throws IOException {
    return new DatasetAdminImpl(datasetContext, spec.getName(), tableUtil, cConf, hConf);
  }

  @Override
  public NoTxKeyValueTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                      Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    return new KVTableImpl(datasetContext, spec.getName(), hConf, tableUtil);
  }

  private static final class DatasetAdminImpl implements DatasetAdmin {
    private final Configuration hConf;
    private final CConfiguration cConf;
    private final TableId tableId;
    protected final HBaseTableUtil tableUtil;

    private DatasetAdminImpl(DatasetContext datasetContext, String tableName, HBaseTableUtil tableUtil,
                             CConfiguration cConf, Configuration hConf) throws IOException {
      this.hConf = hConf;
      this.tableUtil = tableUtil;
      this.tableId = tableUtil.createHTableId(new NamespaceId(datasetContext.getNamespaceId()), tableName);
      this.cConf = cConf;
    }

    @Override
    public boolean exists() throws IOException {
      try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
        return tableUtil.tableExists(admin, tableId);
      }
    }

    @Override
    public void create() throws IOException {
      ColumnFamilyDescriptor.Builder cfdBuilder
        = new ColumnFamilyDescriptor.Builder(hConf, Bytes.toString(DATA_COLUMN_FAMILY));
      TableDescriptor.Builder tblBuilder = new TableDescriptor.Builder(cConf, tableId);
      tblBuilder.addColumnFamily(cfdBuilder.build());
      try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
        tableUtil.createTableIfNotExists(admin, tableId, tblBuilder.build());
      }
    }

    @Override
    public void drop() throws IOException {
      try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
        tableUtil.dropTable(admin, tableId);
      }
    }

    @Override
    public void truncate() throws IOException {
      try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
        tableUtil.truncateTable(admin, tableId);
      }
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

  private static final class KVTableImpl implements NoTxKeyValueTable {
    private static final byte[] DEFAULT_COLUMN = Bytes.toBytes("c");

    private final HBaseTableUtil tableUtil;
    private final HTable table;

    KVTableImpl(DatasetContext datasetContext, String tableName,
                Configuration hConf, HBaseTableUtil tableUtil) throws IOException {
      this.tableUtil = tableUtil;
      TableId tableId = tableUtil.createHTableId(new NamespaceId(datasetContext.getNamespaceId()), tableName);
      this.table = this.tableUtil.createHTable(hConf, tableId);
    }

    @WriteOnly
    @Override
    public void put(byte[] key, @Nullable byte[] value) {
      try {
        if (value == null) {
          table.delete(tableUtil.buildDelete(key).build());
        } else {
          Put put = tableUtil.buildPut(key)
            .add(DATA_COLUMN_FAMILY, DEFAULT_COLUMN, value)
            .build();
          table.put(put);
        }
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @ReadOnly
    @Nullable
    @Override
    public byte[] get(byte[] key) {
      try {
        Result result = table.get(tableUtil.buildGet(key).build());
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
