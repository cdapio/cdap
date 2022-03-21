/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.util.hbase;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.data2.util.TableId;
import io.cdap.cdap.hbase.wd.AbstractRowKeyDistributor;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.hbase.ColumnFamilyDescriptor;
import io.cdap.cdap.spi.hbase.HBaseDDLExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import javax.annotation.Nullable;

/**
 * Common utilities for dealing with HBase.
 */
public abstract class HBaseTableUtil {

  public static final String CDAP_VERSION = "cdap.version";

  public static final String CDAP_HBASE_VERSION = "cdap.hbase.version";

  /**
   * Represents the compression types supported for HBase tables.
   */
  public enum CompressionType {
    LZO, SNAPPY, GZIP, NONE
  }

  /**
   * Represents the bloom filter types supported for HBase tables.
   */
  public enum BloomType {
    ROW, ROWCOL, NONE
  }

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableUtil.class);
  // 4Mb
  public static final int DEFAULT_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;

  private static final CompressionType DEFAULT_COMPRESSION_TYPE = CompressionType.SNAPPY;

  /**
   * This property is ONLY used in test cases.
   */
  public static final String CFG_HBASE_TABLE_COMPRESSION = "hbase.table.compression.default";


  protected String tablePrefix;
  protected CConfiguration cConf;
  protected NamespaceQueryAdmin namespaceQueryAdmin;

  public void setCConf(CConfiguration cConf) {
    this.tablePrefix = getTablePrefix(cConf);
    this.cConf = cConf;
  }

  public void setNamespaceQueryAdmin(NamespaceQueryAdmin namespaceQueryAdmin) {
    if (namespaceQueryAdmin != null) {
      this.namespaceQueryAdmin = namespaceQueryAdmin;
    }
  }

  /**
   * Returns a map of HBase to CDAP namespace. This is required when we want to report metrics for HBase tables where
   * it is run a separate service and reads the table metrics and reports it. There we need to translate the hbase
   * namespace to cdap namespace for metrics to make sense from CDAP perspective. This is also used during upgrade
   * step where we want to construct the correct {@link DatasetAdmin} for each dataset.
   *
   * @return map of hbase namespace to cdap namespace
   * @throws IOException if there was an error getting the {@link NamespaceMeta} of all the namespaces
   */
  public Map<String, String> getHBaseToCDAPNamespaceMap() throws IOException {
    Map<String, String> reverseMap = new HashMap<>();
    if (namespaceQueryAdmin == null) {
      throw new IOException("NamespaceQueryAdmin is not set and a reverseLookupMap was requested.");
    }

    try {
      List<NamespaceMeta> namespaceMetas = namespaceQueryAdmin.list();
      for (NamespaceMeta namespaceMeta : namespaceMetas) {
        String hbaseNamespace = getHBaseNamespace(namespaceMeta);
        reverseMap.put(hbaseNamespace, namespaceMeta.getName());
      }
    } catch (Exception ex) {
      throw new IOException("NamespaceQueryAdmin lookup to list all NamespaceMetas failed", ex);
    }

    return ImmutableMap.copyOf(reverseMap);
  }

  public String getHBaseNamespace(NamespaceId namespaceId) throws IOException {
    // Convert CDAP Namespace to HBase namespace
    if (NamespaceId.SYSTEM.equals(namespaceId) || NamespaceId.CDAP.equals(namespaceId) ||
      NamespaceId.DEFAULT.equals(namespaceId)) {
      return toCDAPManagedHBaseNamespace(namespaceId);
    }

    if (namespaceQueryAdmin == null) {
      throw new IOException(String.format("NamespaceQueryAdmin is not set and a non-reserved namespace " +
                                            "lookup is requested. Namespace %s", namespaceId.getNamespace()));
    }

    try {
      return getHBaseNamespace(namespaceQueryAdmin.get(namespaceId));
    } catch (Exception ex) {
      throw new IOException(String.format("NamespaceQueryAdmin lookup to get NamespaceMeta failed. " +
                                            "Can't find mapping for %s", namespaceId.getNamespace()), ex);
    }
  }

  public String getHBaseNamespace(NamespaceMeta namespaceMeta) {
    if (!Strings.isNullOrEmpty(namespaceMeta.getConfig().getHbaseNamespace())) {
      return namespaceMeta.getConfig().getHbaseNamespace();
    }
    return toCDAPManagedHBaseNamespace(namespaceMeta.getNamespaceId());
  }

  public TableId createHTableId(NamespaceId namespace, String tableName) throws IOException {
    return TableId.from(getHBaseNamespace(namespace), tableName);
  }

  private String toCDAPManagedHBaseNamespace(NamespaceId namespace) {
    // Handle backward compatibility to not add the prefix for default namespace
    // TODO: CDAP-1601 - Conditional should be removed when we have a way to upgrade user datasets
    return NamespaceId.DEFAULT.getEntityName().equals(namespace.getNamespace()) ?
      namespace.getNamespace() : tablePrefix + "_" + namespace.getNamespace();
  }

  protected boolean isCDAPTable(HTableDescriptor hTableDescriptor) {
    return !Strings.isNullOrEmpty(hTableDescriptor.getValue(CDAP_VERSION));
  }

  public HTableDescriptor setVersion(HTableDescriptor tableDescriptor) {
    HTableDescriptorBuilder builder = buildHTableDescriptor(tableDescriptor);
    setVersion(builder);
    return builder.build();
  }

  public HTableDescriptor setHBaseVersion(HTableDescriptor tableDescriptor) {
    HTableDescriptorBuilder builder = buildHTableDescriptor(tableDescriptor);
    setHBaseVersion(builder);
    return builder.build();
  }

  public HTableDescriptor setTablePrefix(HTableDescriptor tableDescriptor) {
    HTableDescriptorBuilder builder = buildHTableDescriptor(tableDescriptor);
    builder.setValue(Constants.Dataset.TABLE_PREFIX, tablePrefix);
    return builder.build();
  }

  /**
   * Get {@link ColumnFamilyDescriptorBuilder} with default properties set.
   * @param columnFamilyName name of the column family
   * @param hConf hadoop configurations
   * @return the builder with default properties set
   */
  public static ColumnFamilyDescriptorBuilder getColumnFamilyDescriptorBuilder(String columnFamilyName,
                                                                               Configuration hConf) {
    ColumnFamilyDescriptorBuilder cfdBuilder = new ColumnFamilyDescriptorBuilder(columnFamilyName);
    String compression = hConf.get(HBaseTableUtil.CFG_HBASE_TABLE_COMPRESSION,
                                   HBaseTableUtil.DEFAULT_COMPRESSION_TYPE.name());
    cfdBuilder
      .setMaxVersions(1)
      .setBloomType(ColumnFamilyDescriptor.BloomType.ROW)
      .setCompressionType(ColumnFamilyDescriptor.CompressionType.valueOf(compression.toUpperCase()));

    return cfdBuilder;
  }

  /**
   * Get {@link TableDescriptorBuilder} with default properties set.
   * @param tableId id of the table for which the descriptor is to be returned
   * @param cConf the instance of the {@link CConfiguration}
   * @return the builder with default properties set
   */
  public static TableDescriptorBuilder getTableDescriptorBuilder(TableId tableId, CConfiguration cConf) {
    String tablePrefix = cConf.get(Constants.Dataset.TABLE_PREFIX);
    TableName tableName = HTableNameConverter.toTableName(tablePrefix, tableId);
    TableDescriptorBuilder tdBuilder = new TableDescriptorBuilder(tableName.getNamespaceAsString(),
                                                                  tableName.getQualifierAsString());
    tdBuilder
      .addProperty(Constants.Dataset.TABLE_PREFIX, tablePrefix)
      .addProperty(HBaseTableUtil.CDAP_VERSION, ProjectInfo.getVersion().toString())
      .addProperty(HBaseTableUtil.CDAP_HBASE_VERSION, HBaseVersion.get().getMajorVersion());


    return tdBuilder;
  }

  public static void setTablePrefix(HTableDescriptorBuilder tableDescriptorBuilder, CConfiguration cConf) {
    tableDescriptorBuilder.setValue(Constants.Dataset.TABLE_PREFIX, getTablePrefix(cConf));
  }

  private static String getTablePrefix(@Nullable CConfiguration cConf) {
    return cConf == null ? null : cConf.get(Constants.Dataset.TABLE_PREFIX);
  }

  public static void setVersion(HTableDescriptorBuilder tableDescriptorBuilder) {
    tableDescriptorBuilder.setValue(CDAP_VERSION, ProjectInfo.getVersion().toString());
  }

  public static ProjectInfo.Version getVersion(HTableDescriptor tableDescriptor) {
    return new ProjectInfo.Version(tableDescriptor.getValue(CDAP_VERSION));
  }

  public static void setHBaseVersion(HTableDescriptorBuilder tableDescriptorBuilder) {
    tableDescriptorBuilder.setValue(CDAP_HBASE_VERSION, HBaseVersion.get().getMajorVersion());
  }

  @Nullable
  public static String getHBaseVersion(HTableDescriptor tableDescriptor) {
    return tableDescriptor.getValue(CDAP_HBASE_VERSION);
  }

  public static byte[][] getSplitKeys(int splits, int buckets, AbstractRowKeyDistributor keyDistributor) {
    return keyDistributor.getSplitKeys(splits, buckets);
  }

  /**
   * Returns information for all coprocessor configured for the table.
   *
   * @return a Map from coprocessor class name to CoprocessorInfo
   */
  public static Map<String, CoprocessorInfo> getCoprocessorInfo(HTableDescriptor tableDescriptor) {
    Map<String, CoprocessorInfo> info = Maps.newHashMap();

    // Extract information about existing data janitor coprocessor
    // The following logic is copied from RegionCoprocessorHost in HBase
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> entry: tableDescriptor.getValues().entrySet()) {
      String key = Bytes.toString(entry.getKey().get()).trim();
      String spec = Bytes.toString(entry.getValue().get()).trim();

      if (!HConstants.CP_HTD_ATTR_KEY_PATTERN.matcher(key).matches()) {
        continue;
      }

      try {
        Matcher matcher = HConstants.CP_HTD_ATTR_VALUE_PATTERN.matcher(spec);
        if (!matcher.matches()) {
          continue;
        }

        String className = matcher.group(2).trim();
        Path path = matcher.group(1).trim().isEmpty() ? null : new Path(matcher.group(1).trim());
        int priority = matcher.group(3).trim().isEmpty() ? Coprocessor.PRIORITY_USER
          : Integer.valueOf(matcher.group(3));
        String cfgSpec = null;
        try {
          cfgSpec = matcher.group(4);
        } catch (IndexOutOfBoundsException ex) {
          // ignore
        }

        Map<String, String> properties = Maps.newHashMap();
        if (cfgSpec != null) {
          cfgSpec = cfgSpec.substring(cfgSpec.indexOf('|') + 1);
          // do an explicit deep copy of the passed configuration
          Matcher m = HConstants.CP_HTD_ATTR_VALUE_PARAM_PATTERN.matcher(cfgSpec);
          while (m.find()) {
            properties.put(m.group(1), m.group(2));
          }
        }
        info.put(className, new CoprocessorInfo(className, path, priority, properties));
      } catch (Exception ex) {
        LOG.warn("Coprocessor attribute '{}' has invalid coprocessor specification '{}'", key, spec, ex);
      }
    }

    return info;
  }

  /**
   * Creates a new {@link Table} which may contain an HBase namespace depending on the HBase version
   *
   * @param conf the hadoop configuration
   * @param tableId the {@link TableId} to create a {@link Table} for
   * @return an {@link Table} for the tableName in the namespace
   */
  public Table createTable(Configuration conf, TableId tableId) throws IOException {
    Preconditions.checkArgument(tableId != null, "Table id should not be null");
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(HTableNameConverter.toTableName(tablePrefix, tableId));
    return new TableWithConnection(connection, table);
  }

  /**
   * Creates a new {@link BufferedMutator} for batch mutation operations.
   *
   * @param table the {@link Table} to have the {@link BufferedMutator} to create on
   * @param writeBufferSize the write buffer size for the buffering
   * @return a {@link BufferedMutator}
   * @throws IOException if failed to create connection to HBase
   */
  public BufferedMutator createBufferedMutator(Table table, long writeBufferSize) throws IOException {
    TableName tableName = table.getTableDescriptor().getTableName();

    BufferedMutatorParams params = new BufferedMutatorParams(tableName)
      .writeBufferSize(writeBufferSize);

    // Try to reuse the connection from the Table
    if (table instanceof TableWithConnection) {
      Connection connection = ((TableWithConnection) table).acquireConnection();
      if (connection != null) {
        return new DelegatingBufferedMutator(connection.getBufferedMutator(params)) {
          @Override
          public void close() throws IOException {
            try {
              super.close();
            } finally {
              ((TableWithConnection) table).releaseConnection();
            }
          }
        };
      }
    }

    // If cannot get a connection from the given table, create a new one and use it to create BufferedMutator
    Connection connection = ConnectionFactory.createConnection(table.getConfiguration());
    return new DelegatingBufferedMutator(connection.getBufferedMutator(params)) {
      @Override
      public void close() throws IOException {
        try {
          super.close();
        } finally {
          connection.close();
        }
      }
    };
  }

  /**
   * Creates a new {@link HTableDescriptorBuilder} which may contain an HBase namespace depending on the HBase version
   *
   * @param tableId the {@link TableId} to create an {@link HTableDescriptor} for
   * @return an {@link HTableDescriptorBuilder} for the table
   */
  public abstract HTableDescriptorBuilder buildHTableDescriptor(TableId tableId);

  /**
   * Creates a new {@link HTableDescriptorBuilder} which may contain an HBase namespace depending on the HBase version
   *
   * @param tableDescriptor the {@link HTableDescriptor} whose values should be copied
   * @return an {@link HTableDescriptorBuilder} for the table
   */
  public abstract HTableDescriptorBuilder buildHTableDescriptor(HTableDescriptor tableDescriptor);

  /**
   * Constructs a {@link HTableDescriptor} which may contain an HBase namespace for an existing table
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param tableId the {@link TableId} to construct an {@link HTableDescriptor} for
   * @return an {@link HTableDescriptor} for the table
   * @throws IOException if failed to get the table descriptor
   */
  public abstract HTableDescriptor getHTableDescriptor(HBaseAdmin admin, TableId tableId) throws IOException;

  /**
   * Checks if an HBase namespace already exists
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param namespace the namespace to check for existence
   * @throws IOException if an I/O error occurs during the operation
   */
  public abstract boolean hasNamespace(HBaseAdmin admin, String namespace) throws IOException;

  /**
   * Check if an HBase table exists
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param tableId {@link TableId} for the specified table
   * @throws IOException if failed to connect to HBase
   */
  public abstract boolean tableExists(HBaseAdmin admin, TableId tableId) throws IOException;

  /**
   * Delete an HBase table
   *
   * @param ddlExecutor the {@link HBaseDDLExecutor} to use to communicate with HBase
   * @param tableId {@link TableId} for the specified table
   * @throws IOException if failed to connect to HBase
   */
  public abstract void deleteTable(HBaseDDLExecutor ddlExecutor, TableId tableId) throws IOException;

  /**
   * Modify an HBase table
   *
   * @param ddlExecutor the {@link HBaseDDLExecutor} to use to communicate with HBase
   * @param tableDescriptor the modified {@link HTableDescriptor}
   * @throws IOException if failed to connect to HBase
   */
  public abstract void modifyTable(HBaseDDLExecutor ddlExecutor, HTableDescriptor tableDescriptor) throws IOException;

  /**
   * Returns a list of {@link HRegionInfo} for the specified {@link TableId}
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param tableId {@link TableId} for the specified table
   * @return a list of {@link HRegionInfo} for the specified {@link TableId}
   * @throws IOException if failed to connect to HBase
   */
  public abstract List<HRegionInfo> getTableRegions(HBaseAdmin admin, TableId tableId) throws IOException;

  /**
   * Deletes all tables in the specified namespace that satisfy the given {@link Predicate}.
   *
   * @param ddlExecutor the {@link HBaseAdmin} to use to communicate with HBase
   * @param namespaceId namespace for which the tables are being deleted
   * @param hConf The {@link Configuration} instance
   * @param predicate The {@link Predicate} to decide whether to drop a table or not
   * @throws IOException if failed to connect to HBase
   */
  public void deleteAllInNamespace(HBaseDDLExecutor ddlExecutor, String namespaceId,
                                   Configuration hConf, Predicate<TableId> predicate) throws IOException {
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      for (TableId tableId : listTablesInNamespace(admin, namespaceId)) {
        if (predicate.apply(tableId)) {
          dropTable(ddlExecutor, tableId);
        }
      }
    }
  }

  /**
   * Deletes all tables in the specified namespace
   *
   * @param ddlExecutor the {@link HBaseDDLExecutor} to use to communicate with HBase
   * @param namespaceId namespace for which the tables are being deleted
   * @param hConf The {@link Configuration} instance
   * @throws IOException if failed to connect to HBase
   */
  public void deleteAllInNamespace(HBaseDDLExecutor ddlExecutor, String namespaceId, Configuration hConf)
    throws IOException {
    deleteAllInNamespace(ddlExecutor, namespaceId, hConf, Predicates.alwaysTrue());
  }

  /**
   * Lists all tables in the specified namespace
   *
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   * @param namespaceId HBase namespace for which the tables are being requested
   */
  public abstract List<TableId> listTablesInNamespace(HBaseAdmin admin, String namespaceId) throws IOException;

  /**
   * Lists all tables
   * @param admin the {@link HBaseAdmin} to use to communicate with HBase
   */
  public abstract List<TableId> listTables(HBaseAdmin admin) throws IOException;

  /**
   * Disables and deletes a table.
   * @param ddlExecutor the {@link HBaseDDLExecutor} to use to communicate with HBase
   * @param tableId  {@link TableId} for the specified table
   * @throws IOException if failed to connect to HBase
   */
  public void dropTable(HBaseDDLExecutor ddlExecutor, TableId tableId) throws IOException {
    TableName tableName = HTableNameConverter.toTableName(getTablePrefix(cConf), tableId);
    ddlExecutor.disableTableIfEnabled(tableName.getNamespaceAsString(), tableName.getQualifierAsString());
    deleteTable(ddlExecutor, tableId);
  }

  /**
   * Truncates a table.
   * @param ddlExecutor the {@link HBaseDDLExecutor} to use to communicate with HBase
   * @param tableId  {@link TableId} for the specified table
   * @throws IOException if failed to connect to HBase
   */
  public void truncateTable(HBaseDDLExecutor ddlExecutor, TableId tableId) throws IOException {
    TableName tableName = HTableNameConverter.toTableName(getTablePrefix(cConf), tableId);
    ddlExecutor.truncateTable(tableName.getNamespaceAsString(), tableName.getQualifierAsString());
  }

  /**
   * Grants permissions on a table.
   * @param ddlExecutor the {@link HBaseDDLExecutor} to use to communicate with HBase
   * @param tableId  {@link TableId} for the specified table
   * @param permissions A map from user or group name to the permissions for that user or group, given as a string
   *                    containing only characters 'a'(Admin), 'c'(Create), 'r'(Read), 'w'(Write), and 'x'(Execute).
   *                    Group names must be prefixed with the character '@'.
   * @throws IOException if failed to connect to HBase
   */
  public void grantPermissions(HBaseDDLExecutor ddlExecutor, TableId tableId,
                               Map<String, String> permissions) throws IOException {
    TableName tableName = HTableNameConverter.toTableName(getTablePrefix(cConf), tableId);
    ddlExecutor.grantPermissions(tableName.getNamespaceAsString(), tableName.getQualifierAsString(), permissions);
  }

  /**
   * Creates a {@link ScanBuilder}.
   */
  public ScanBuilder buildScan() {
    return new DefaultScanBuilder();
  }

  /**
   * Creates a {@link ScanBuilder} by copying from another {@link Scan} instance.
   */
  public ScanBuilder buildScan(Scan scan) throws IOException {
    return new DefaultScanBuilder(scan);
  }

  /**
   * Creates a {@link PutBuilder} for the given row.
   */
  public PutBuilder buildPut(byte[] row) {
    return new DefaultPutBuilder(row);
  }

  /**
   * Creates a {@link IncrementBuilder} for the given row.
   */
  public IncrementBuilder buildIncrement(byte[] row) {
    return new DefaultIncrementBuilder(row);
  }

  /**
   * Creates a {@link PutBuilder} by copying from another {@link Put} instance.
   */
  public PutBuilder buildPut(Put put) {
    return new DefaultPutBuilder(put);
  }

  /**
   * Creates a {@link GetBuilder} for the given row.
   */
  public GetBuilder buildGet(byte[] row) {
    return new DefaultGetBuilder(row);
  }

  /**
   * Creates a {@link GetBuilder} by copying from another {@link Get} instance.
   */
  public GetBuilder buildGet(Get get) {
    return new DefaultGetBuilder(get);
  }

  /**
   * Creates a {@link DeleteBuilder} for the given row.
   */
  public DeleteBuilder buildDelete(byte[] row) {
    return new DefaultDeleteBuilder(row);
  }

  /**
   * Creates a {@link DeleteBuilder} by copying from another {@link Delete} instance.
   */
  public DeleteBuilder buildDelete(Delete delete) {
    return new DefaultDeleteBuilder(delete);
  }

  public abstract void setCompression(HColumnDescriptor columnDescriptor, CompressionType type);

  public abstract void setBloomFilter(HColumnDescriptor columnDescriptor, BloomType type);

  public abstract CompressionType getCompression(HColumnDescriptor columnDescriptor);

  public abstract BloomType getBloomFilter(HColumnDescriptor columnDescriptor);

  public abstract boolean isGlobalAdmin(Configuration hConf) throws IOException;

  public abstract Class<? extends Coprocessor> getTransactionDataJanitorClassForVersion();
  public abstract Class<? extends Coprocessor> getIncrementHandlerClassForVersion();
  public abstract Class<? extends Coprocessor> getMessageTableRegionObserverClassForVersion();
  public abstract Class<? extends Coprocessor> getPayloadTableRegionObserverClassForVersion();

  /**
   * Collects HBase table stats
   * //TODO: Explore the possiblitity of returning a {@code Map<TableId, TableStats>}
   * @param admin instance of {@link HBaseAdmin} to communicate with HBase
   * @return map of table name -> table stats
   * @throws IOException if failed to connect to HBase
   */
  public Map<TableId, TableStats> getTableStats(HBaseAdmin admin) throws IOException {
    // The idea is to walk thru live region servers, collect table region stats and aggregate them towards table total
    // metrics.
    Map<TableId, TableStats> datasetStat = Maps.newHashMap();
    ClusterStatus clusterStatus = admin.getClusterStatus();

    for (ServerName serverName : clusterStatus.getServers()) {
      Map<byte[], RegionLoad> regionsLoad = clusterStatus.getLoad(serverName).getRegionsLoad();

      for (RegionLoad regionLoad : regionsLoad.values()) {
        TableName tableName = HRegionInfo.getTable(regionLoad.getName());
        HTableDescriptor tableDescriptor;
        try {
          tableDescriptor = admin.getTableDescriptor(tableName);
        } catch (TableNotFoundException exception) {
          // this can happen if the table has been deleted; the region stats get removed afterwards
          LOG.warn("Table not found for table name {}. Skipping collecting stats for it. Reason: {}",
                   tableName, exception.getMessage());
          continue;
        }
        if (!isCDAPTable(tableDescriptor)) {
          continue;
        }
        TableId tableId = HTableNameConverter.from(tableDescriptor);
        TableStats stat = datasetStat.get(tableId);
        if (stat == null) {
          stat = new TableStats(regionLoad.getStorefileSizeMB(), regionLoad.getMemStoreSizeMB());
          datasetStat.put(tableId, stat);
        } else {
          stat.incStoreFileSizeMB(regionLoad.getStorefileSizeMB());
          stat.incMemStoreSizeMB(regionLoad.getMemStoreSizeMB());
        }
      }
    }
    return datasetStat;
  }

  protected void warnGlobalAdminCheckFailure() {
    LOG.warn("Unable to determine if cdap is a global admin or not. Failing back to {} configuration.",
             Constants.Startup.TX_PRUNE_ACL_CHECK);
  }

  /**
   * Carries information about table stats
   */
  public static final class TableStats {
    private int storeFileSizeMB;
    private int memStoreSizeMB;

    TableStats(int storeFileSizeMB, int memStoreSizeMB) {
      this.storeFileSizeMB = storeFileSizeMB;
      this.memStoreSizeMB = memStoreSizeMB;
    }

    public int getStoreFileSizeMB() {
      return storeFileSizeMB;
    }

    public int getMemStoreSizeMB() {
      return memStoreSizeMB;
    }

    void incStoreFileSizeMB(int deltaMB) {
      this.storeFileSizeMB += deltaMB;
    }

    void incMemStoreSizeMB(int deltaMB) {
      this.memStoreSizeMB += deltaMB;
    }

    public int getTotalSizeMB() {
      // both memstore and size on fs contribute to size of the dataset, otherwise user will be confused with zeroes
      // in dataset size even after something was written...
      return storeFileSizeMB + memStoreSizeMB;
    }
  }

  /**
   * Carries information about coprocessor information.
   */
  public static final class CoprocessorInfo {
    private final String className;
    private final Path path;
    private final int priority;
    private final Map<String, String> properties;

    private CoprocessorInfo(String className, Path path, int priority, Map<String, String> properties) {
      this.className = className;
      this.path = path;
      this.priority = priority;
      this.properties = ImmutableMap.copyOf(properties);
    }

    public String getClassName() {
      return className;
    }

    public Path getPath() {
      return path;
    }

    public int getPriority() {
      return priority;
    }

    public Map<String, String> getProperties() {
      return properties;
    }
  }

  /**
   * A HBase {@link Table} wrapper that provides access to the {@link Connection} for creating the table.
   */
  private static class TableWithConnection extends DelegatingTable {

    private final Connection connection;
    private final AtomicInteger connectionCount;

    TableWithConnection(Connection connection, Table delegate) {
      super(delegate);
      this.connection = connection;
      this.connectionCount = new AtomicInteger(1);
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        releaseConnection();
      }
    }

    /**
     * Acquires the usage of the current connection.
     *
     * @return the {@link Connection} to use or {@code null} if the connection is already closed.
     */
    @Nullable
    Connection acquireConnection() {
      int oldCount;
      do {
        oldCount = connectionCount.get();
        if (oldCount == 0) {
          return null;
        }
      } while (!connectionCount.compareAndSet(oldCount, oldCount + 1));
      return connection;
    }

    void releaseConnection() throws IOException {
      int useCount = connectionCount.decrementAndGet();
      if (useCount == 0) {
        connection.close();
      } else {
        LOG.trace("HBase connection is still in used by {} clients", useCount);
      }
    }
  }
}
