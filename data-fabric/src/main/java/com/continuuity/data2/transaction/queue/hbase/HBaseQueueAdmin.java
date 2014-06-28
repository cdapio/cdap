package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset.lib.hbase.AbstractHBaseDataSetManager;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.QueueConstants;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.continuuity.hbase.wd.AbstractRowKeyDistributor;
import com.continuuity.hbase.wd.RowKeyDistributorByHashPrefix;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import static com.continuuity.data2.transaction.queue.QueueConstants.QueueType.QUEUE;

/**
 * admin for queues in hbase.
 */
@Singleton
public class HBaseQueueAdmin extends AbstractHBaseDataSetManager implements QueueAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueueAdmin.class);

  public static final int SALT_BYTES = 1;
  public static final int ROW_KEY_DISTRIBUTION_BUCKETS = 8;
  public static final AbstractRowKeyDistributor ROW_KEY_DISTRIBUTOR =
    new RowKeyDistributorByHashPrefix(
      new RowKeyDistributorByHashPrefix.OneByteSimpleHash(ROW_KEY_DISTRIBUTION_BUCKETS));

  private final CConfiguration cConf;
  private final LocationFactory locationFactory;
  private final String tableNamePrefix;
  private final String configTableName;
  private final QueueConstants.QueueType type;

  @Inject
  public HBaseQueueAdmin(Configuration hConf,
                         CConfiguration cConf,
                         DataSetAccessor dataSetAccessor,
                         LocationFactory locationFactory,
                         HBaseTableUtil tableUtil) throws IOException {
    this(hConf, cConf, QUEUE, dataSetAccessor, locationFactory, tableUtil);
  }

  protected HBaseQueueAdmin(Configuration hConf,
                            CConfiguration cConf,
                            QueueConstants.QueueType type,
                            DataSetAccessor dataSetAccessor,
                            LocationFactory locationFactory,
                            HBaseTableUtil tableUtil) throws IOException {
    super(hConf, tableUtil);
    this.cConf = cConf;
    // todo: we have to do that because queues do not follow dataset semantic fully (yet)
    String unqualifiedTableNamePrefix =
      type == QUEUE ? QueueConstants.QUEUE_TABLE_PREFIX : QueueConstants.STREAM_TABLE_PREFIX;
    this.type = type;
    this.tableNamePrefix = HBaseTableUtil.getHBaseTableName(
      dataSetAccessor.namespace(unqualifiedTableNamePrefix, DataSetAccessor.Namespace.SYSTEM));
    this.configTableName = HBaseTableUtil.getHBaseTableName(
      dataSetAccessor.namespace(QueueConstants.QUEUE_CONFIG_TABLE_NAME, DataSetAccessor.Namespace.SYSTEM));
    this.locationFactory = locationFactory;
  }

  /**
   * This determines the actual table name from the table name prefix and the name of the queue.
   * @param queueName The name of the queue.
   * @return the full name of the table that holds this queue.
   */
  public String getActualTableName(QueueName queueName) {
    if (queueName.isQueue()) {
      // <reactor namespace>.system.queue.<account>.<flow>
      return getTableNameForFlow(queueName.getFirstComponent(), queueName.getSecondComponent());
    } else {
      throw new IllegalArgumentException("'" + queueName + "' is not a valid name for a queue.");
    }
  }

  private String getTableNameForFlow(String app, String flow) {
    return tableNamePrefix + "." + app + "." + flow;
  }

  /**
   * This determines whether dropping a queue is supported (by dropping the queue's table).
   */
  public boolean doDropTable(@SuppressWarnings("unused") QueueName queueName) {
    // no-op because this would drop all tables for the flow
    // todo: introduce a method dropAllFor(flow) or similar
    return false;
  }

  /**
   * This determines whether truncating a queue is supported (by truncating the queue's table).
   */
  public boolean doTruncateTable(@SuppressWarnings("unused") QueueName queueName) {
    // yes, this will truncate all queues of the flow. But it rarely makes sense to clear a single queue.
    // todo: introduce a method truncateAllFor(flow) or similar, and set this to false
    return true;
  }

  /**
   * Returns the column qualifier for the consumer state column. The qualifier is formed by
   * {@code <groupId><instanceId>}.
   * @param groupId Group ID of the consumer
   * @param instanceId Instance ID of the consumer
   * @return A new byte[] which is the column qualifier.
   */
  public static byte[] getConsumerStateColumn(long groupId, int instanceId) {
    byte[] column = new byte[Longs.BYTES + Ints.BYTES];
    Bytes.putLong(column, 0, groupId);
    Bytes.putInt(column, Longs.BYTES, instanceId);
    return column;
  }

  /**
   * @param queueTableName actual queue table name
   * @return app name this queue belongs to
   */
  public static String getApplicationName(String queueTableName) {
    // last two parts are appName and flow
    String[] parts = queueTableName.split("\\.");
    return parts[parts.length - 2];
  }

  /**
   * @param queueTableName actual queue table name
   * @return flow name this queue belongs to
   */
  public static String getFlowName(String queueTableName) {
    // last two parts are appName and flow
    String[] parts = queueTableName.split("\\.");
    return parts[parts.length - 1];
  }

  @Override
  public boolean exists(String name) throws Exception {
    return exists(QueueName.from(URI.create(name)));
  }

  boolean exists(QueueName queueName) throws IOException {
    HBaseAdmin admin = getHBaseAdmin();
    return admin.tableExists(getActualTableName(queueName)) && admin.tableExists(configTableName);
  }

  @Override
  public void create(String name, @SuppressWarnings("unused") Properties props) throws Exception {
    create(name);
  }

  @Override
  public void create(String name) throws IOException {
    create(QueueName.from(URI.create(name)));
  }

  @Override
  public void truncate(String name) throws Exception {
    QueueName queueName = QueueName.from(URI.create(name));
    // all queues for one flow are stored in same table, and we would clear all of them. this makes it optional.
    if (doTruncateTable(queueName)) {
      byte[] tableNameBytes = Bytes.toBytes(getActualTableName(queueName));
      truncate(tableNameBytes);
    } else {
      LOG.warn("truncate({}) on HBase queue table has no effect.", name);
    }
    // we can delete the config for this queue in any case.
    deleteConsumerConfigurations(queueName);
  }

  private void truncate(byte[] tableNameBytes) throws IOException {
    HBaseAdmin admin = getHBaseAdmin();
    if (admin.tableExists(tableNameBytes)) {
      HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableNameBytes);
      admin.disableTable(tableNameBytes);
      admin.deleteTable(tableNameBytes);
      admin.createTable(tableDescriptor);
    }
  }

  @Override
  public void clearAllForFlow(String app, String flow) throws Exception {
    // all queues for a flow are in one table
    String tableName = getTableNameForFlow(app, flow);
    truncate(Bytes.toBytes(tableName));
    // we also have to delete the config for these queues
    deleteConsumerConfigurations(app, flow);
  }

  @Override
  public void dropAllForFlow(String app, String flow) throws Exception {
    // all queues for a flow are in one table
    String tableName = getTableNameForFlow(app, flow);
    drop(Bytes.toBytes(tableName));
    // we also have to delete the config for these queues
    deleteConsumerConfigurations(app, flow);
  }

  @Override
  public void drop(String name) throws Exception {
    QueueName queueName = QueueName.from(URI.create(name));
    // all queues for one flow are stored in same table, and we would drop all of them. this makes it optional.
    if (doDropTable(queueName)) {
      byte[] tableNameBytes = Bytes.toBytes(getActualTableName(queueName));
      drop(tableNameBytes);
    } else {
      LOG.warn("drop({}) on HBase queue table has no effect.", name);
    }
    // we can delete the config for this queue in any case.
    deleteConsumerConfigurations(queueName);
  }

  private void drop(byte[] tableName) throws IOException {
    HBaseAdmin admin = getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }

  private void deleteConsumerConfigurations(QueueName queueName) throws IOException {
    // we need to delete the row for this queue name from the config table
    HTable hTable = new HTable(getHBaseAdmin().getConfiguration(), configTableName);
    try {
      byte[] rowKey = queueName.toBytes();
      hTable.delete(new Delete(rowKey));
    } finally {
      hTable.close();
    }
  }

  private void deleteConsumerConfigurations(String app, String flow) throws IOException {
    // table is created lazily, possible it may not exist yet.
    HBaseAdmin admin = getHBaseAdmin();
    if (admin.tableExists(configTableName)) {
      // we need to delete the row for this queue name from the config table
      HTable hTable = new HTable(admin.getConfiguration(), configTableName);
      try {
        byte[] prefix = Bytes.toBytes(QueueName.prefixForFlow(app, flow));
        byte[] stop = Arrays.copyOf(prefix, prefix.length);
        stop[prefix.length - 1]++; // this is safe because the last byte is always '/'

        Scan scan = new Scan();
        scan.setStartRow(prefix);
        scan.setStopRow(stop);
        scan.setFilter(new FirstKeyOnlyFilter());
        scan.setMaxVersions(1);
        ResultScanner resultScanner = hTable.getScanner(scan);

        List<Delete> deletes = Lists.newArrayList();
        Result result;
        try {
          while ((result = resultScanner.next()) != null) {
            byte[] row = result.getRow();
            deletes.add(new Delete(row));
          }
        } finally {
          resultScanner.close();
        }

        hTable.delete(deletes);

      } finally {
        hTable.close();
      }
    }
  }

  @Override
  protected String getHBaseTableName(String name) {
    QueueName queueName = QueueName.from(URI.create(name));
    return getActualTableName(queueName);
  }

  @Override
  protected CoprocessorJar createCoprocessorJar() throws IOException {
    List<? extends Class<? extends Coprocessor>> coprocessors = getCoprocessors();
    if (coprocessors.isEmpty()) {
      return CoprocessorJar.EMPTY;
    }

    Location jarDir = locationFactory.create(cConf.get(QueueConstants.ConfigKeys.QUEUE_TABLE_COPROCESSOR_DIR,
                                                       QueueConstants.DEFAULT_QUEUE_TABLE_COPROCESSOR_DIR));
    Location jarFile = HBaseTableUtil.createCoProcessorJar(type.name().toLowerCase(), jarDir, coprocessors);
    return new CoprocessorJar(coprocessors, jarFile);
  }

  @Override
  protected boolean upgradeTable(HTableDescriptor tableDescriptor, Properties properties) {
    HColumnDescriptor columnDescriptor = tableDescriptor.getFamily(QueueEntryRow.COLUMN_FAMILY);
    if (columnDescriptor.getMaxVersions() != 1) {
      columnDescriptor.setMaxVersions(1);
      return true;
    }
    return false;
  }

  public void create(QueueName queueName) throws IOException {
    // Queue Config needs to be on separate table, otherwise disabling the queue table would makes queue config
    // not accessible by the queue region coprocessor for doing eviction.

    // Create the config table first so that in case the queue table coprocessor runs, it can access the config table.
    createConfigTable();

    // Create the queue table
    byte[] tableName = Bytes.toBytes(getActualTableName(queueName));
    HTableDescriptor htd = new HTableDescriptor(tableName);

    HColumnDescriptor hcd = new HColumnDescriptor(QueueEntryRow.COLUMN_FAMILY);
    htd.addFamily(hcd);
    hcd.setMaxVersions(1);

    // Add coprocessors
    CoprocessorJar coprocessorJar = createCoprocessorJar();
    for (Class<? extends Coprocessor> coprocessor : coprocessorJar.getCoprocessors()) {
      addCoprocessor(htd, coprocessor, coprocessorJar.getJarLocation());
    }

    // Create queue table with splits.
    int splits = cConf.getInt(QueueConstants.ConfigKeys.QUEUE_TABLE_PRESPLITS,
                              QueueConstants.DEFAULT_QUEUE_TABLE_PRESPLITS);
    byte[][] splitKeys = HBaseTableUtil.getSplitKeys(splits);

    tableUtil.createTableIfNotExists(getHBaseAdmin(), tableName, htd, splitKeys);
  }

  private void createConfigTable() throws IOException {
    byte[] tableName = Bytes.toBytes(configTableName);
    HTableDescriptor htd = new HTableDescriptor(tableName);

    HColumnDescriptor hcd = new HColumnDescriptor(QueueEntryRow.COLUMN_FAMILY);
    htd.addFamily(hcd);
    hcd.setMaxVersions(1);

    tableUtil.createTableIfNotExists(getHBaseAdmin(), tableName, htd, null,
                                     QueueConstants.MAX_CREATE_TABLE_WAIT, TimeUnit.MILLISECONDS);
  }

  /**
   * @return coprocessors to set for the {@link HTable}
   */
  protected List<? extends Class<? extends Coprocessor>> getCoprocessors() {
    return ImmutableList.of(tableUtil.getQueueRegionObserverClassForVersion(),
                            tableUtil.getDequeueScanObserverClassForVersion());
  }

  @Override
  public void dropAll() throws Exception {
    for (HTableDescriptor desc : getHBaseAdmin().listTables()) {
      String tableName = Bytes.toString(desc.getName());
      // It's important to keep config table enabled while disabling queue tables.
      if (tableName.startsWith(tableNamePrefix) && !tableName.equals(configTableName)) {
        drop(desc.getName());
      }
    }
    // drop config table last
    drop(Bytes.toBytes(configTableName));
  }

  @Override
  public void configureInstances(QueueName queueName, long groupId, int instances) throws Exception {
    Preconditions.checkArgument(instances > 0, "Number of consumer instances must be > 0.");

    if (!exists(queueName)) {
      create(queueName);
    }

    HTable hTable = new HTable(getHBaseAdmin().getConfiguration(), configTableName);

    try {
      byte[] rowKey = queueName.toBytes();

      // Get all latest entry row key of all existing instances
      // Consumer state column is named as "<groupId><instanceId>"
      Get get = new Get(rowKey);
      get.addFamily(QueueEntryRow.COLUMN_FAMILY);
      get.setFilter(new ColumnPrefixFilter(Bytes.toBytes(groupId)));
      List<HBaseConsumerState> consumerStates = HBaseConsumerState.create(hTable.get(get));

      int oldInstances = consumerStates.size();

      // Nothing to do if size doesn't change
      if (oldInstances == instances) {
        return;
      }
      // Compute and applies changes
      hTable.batch(getConfigMutations(groupId, instances, rowKey, consumerStates, new ArrayList<Mutation>()));

    } finally {
      hTable.close();
    }
  }

  @Override
  public void configureGroups(QueueName queueName, Map<Long, Integer> groupInfo) throws Exception {
    Preconditions.checkArgument(!groupInfo.isEmpty(), "Consumer group information must not be empty.");

    if (!exists(queueName)) {
      create(queueName);
    }

    HTable hTable = new HTable(getHBaseAdmin().getConfiguration(), configTableName);

    try {
      byte[] rowKey = queueName.toBytes();

      // Get the whole row
      Result result = hTable.get(new Get(rowKey));

      // Generate existing groupInfo, also find smallest rowKey from existing group if there is any
      NavigableMap<byte[], byte[]> columns = result.getFamilyMap(QueueEntryRow.COLUMN_FAMILY);
      if (columns == null) {
        columns = ImmutableSortedMap.of();
      }
      Map<Long, Integer> oldGroupInfo = Maps.newHashMap();
      byte[] smallest = decodeGroupInfo(groupInfo, columns, oldGroupInfo);

      List<Mutation> mutations = Lists.newArrayList();

      // For groups that are removed, simply delete the columns
      Sets.SetView<Long> removedGroups = Sets.difference(oldGroupInfo.keySet(), groupInfo.keySet());
      if (!removedGroups.isEmpty()) {
        Delete delete = new Delete(rowKey);
        for (long removeGroupId : removedGroups) {
          for (int i = 0; i < oldGroupInfo.get(removeGroupId); i++) {
            delete.deleteColumns(QueueEntryRow.COLUMN_FAMILY,
                                 getConsumerStateColumn(removeGroupId, i));
          }
        }
        mutations.add(delete);
      }

      // For each group that changed (either a new group or number of instances change), update the startRow
      Put put = new Put(rowKey);
      for (Map.Entry<Long, Integer> entry : groupInfo.entrySet()) {
        long groupId = entry.getKey();
        int instances = entry.getValue();
        if (!oldGroupInfo.containsKey(groupId)) {
          // For new group, simply put with smallest rowKey from other group or an empty byte array if none exists.
          for (int i = 0; i < instances; i++) {
            put.add(QueueEntryRow.COLUMN_FAMILY,
                    getConsumerStateColumn(groupId, i),
                    smallest == null ? Bytes.EMPTY_BYTE_ARRAY : smallest);
          }
        } else if (oldGroupInfo.get(groupId) != instances) {
          // compute the mutations needed using the change instances logic
          SortedMap<byte[], byte[]> columnMap =
            columns.subMap(getConsumerStateColumn(groupId, 0),
                           getConsumerStateColumn(groupId, oldGroupInfo.get(groupId)));

          mutations = getConfigMutations(groupId, instances, rowKey, HBaseConsumerState.create(columnMap), mutations);
        }
      }
      mutations.add(put);

      // Compute and applies changes
      if (!mutations.isEmpty()) {
        hTable.batch(mutations);
      }

    } finally {
      hTable.close();
    }
  }

  @Override
  public void upgrade() throws Exception {
    // For each table managed by this admin, performs an upgrade
    Properties properties = new Properties();
    for (HTableDescriptor desc : getHBaseAdmin().listTables()) {
      String tableName = Bytes.toString(desc.getName());
      // It's important to skip config table enabled.
      if (tableName.startsWith(tableNamePrefix) && !tableName.equals(configTableName)) {
        upgradeTable(tableName, properties);
      }
    }
  }

  private byte[] decodeGroupInfo(Map<Long, Integer> groupInfo,
                                 Map<byte[], byte[]> columns, Map<Long, Integer> oldGroupInfo) {
    byte[] smallest = null;

    for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
      // Consumer state column is named as "<groupId><instanceId>"
      long groupId = Bytes.toLong(entry.getKey());

      // Map key is sorted by groupId then instanceId, hence keep putting the instance + 1 will gives the group size.
      oldGroupInfo.put(groupId, Bytes.toInt(entry.getKey(), Longs.BYTES) + 1);

      // Update smallest if the group still exists from the new groups.
      if (groupInfo.containsKey(groupId)
          && (smallest == null || Bytes.BYTES_COMPARATOR.compare(entry.getValue(), smallest) < 0)) {
        smallest = entry.getValue();
      }
    }
    return smallest;
  }

  private List<Mutation> getConfigMutations(long groupId, int instances, byte[] rowKey,
                                            List<HBaseConsumerState> consumerStates, List<Mutation> mutations) {
    // Find smallest startRow among existing instances
    byte[] smallest = null;
    for (HBaseConsumerState consumerState : consumerStates) {
      if (smallest == null || Bytes.BYTES_COMPARATOR.compare(consumerState.getStartRow(), smallest) < 0) {
        smallest = consumerState.getStartRow();
      }
    }
    Preconditions.checkArgument(smallest != null, "No startRow found for consumer group %s", groupId);

    int oldInstances = consumerStates.size();

    // When group size changed, reset all instances startRow to smallest startRow
    Put put = new Put(rowKey);
    Delete delete = new Delete(rowKey);
    for (HBaseConsumerState consumerState : consumerStates) {
      HBaseConsumerState newState = new HBaseConsumerState(smallest,
                                                           consumerState.getGroupId(),
                                                           consumerState.getInstanceId());
      if (consumerState.getInstanceId() < instances) {
        // Updates to smallest rowKey
        newState.updatePut(put);
      } else {
        // Delete old instances
        newState.delete(delete);
      }
    }
    // For all new instances, set startRow to smallest
    for (int i = oldInstances; i < instances; i++) {
      new HBaseConsumerState(smallest, groupId, i).updatePut(put);
    }
    if (!put.isEmpty()) {
      mutations.add(put);
    }
    if (!delete.isEmpty()) {
      mutations.add(delete);
    }

    return mutations;
  }

  protected String getTableNamePrefix() {
    return tableNamePrefix;
  }

  public String getConfigTableName() {
    return configTableName;
  }
}
