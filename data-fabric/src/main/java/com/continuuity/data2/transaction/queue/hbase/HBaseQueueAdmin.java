package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.QueueConstants;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
import com.continuuity.data2.transaction.queue.hbase.coprocessor.DequeueScanObserver;
import com.continuuity.data2.transaction.queue.hbase.coprocessor.HBaseQueueRegionObserver;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.continuuity.hbase.wd.AbstractRowKeyDistributor;
import com.continuuity.hbase.wd.RowKeyDistributorByHashPrefix;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
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

import static com.continuuity.data2.transaction.queue.QueueConstants.QueueType.QUEUE;

/**
 * admin for queues in hbase.
 */
@Singleton
public class HBaseQueueAdmin implements QueueAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueueAdmin.class);

  public static final int SALT_BYTES = 1;
  public static final int ROW_KEY_DISTRIBUTION_BUCKETS = 8;
  public static final AbstractRowKeyDistributor ROW_KEY_DISTRIBUTOR =
    new RowKeyDistributorByHashPrefix(
      new RowKeyDistributorByHashPrefix.OneByteSimpleHash(ROW_KEY_DISTRIBUTION_BUCKETS));
  public static final Class<DequeueScanObserver> DEQUEUE_CP = DequeueScanObserver.class;
  private static final Class[] COPROCESSORS = new Class[]{HBaseQueueRegionObserver.class, DEQUEUE_CP};

  private final HBaseAdmin admin;
  private final CConfiguration cConf;
  private final LocationFactory locationFactory;
  private final String tableNamePrefix;
  private final String configTableName;

  @Inject
  public HBaseQueueAdmin(@Named("HBaseOVCTableHandleHConfig") Configuration hConf,
                         @Named("HBaseOVCTableHandleCConfig") CConfiguration cConf,
                         DataSetAccessor dataSetAccessor,
                         LocationFactory locationFactory) throws IOException {
    this(hConf, cConf, QUEUE, dataSetAccessor, locationFactory);
  }

  protected HBaseQueueAdmin(Configuration hConf,
                            CConfiguration cConf,
                            QueueConstants.QueueType type,
                            DataSetAccessor dataSetAccessor,
                            LocationFactory locationFactory) throws IOException {
    this.admin = new HBaseAdmin(hConf);
    this.cConf = cConf;
    // todo: we have to do that because queues do not follow dataset semantic fully (yet)
    String unqualifiedTableNamePrefix =
      type == QUEUE ? QueueConstants.QUEUE_TABLE_PREFIX : QueueConstants.STREAM_TABLE_PREFIX;
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
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }

  private void deleteConsumerConfigurations(QueueName queueName) throws IOException {
    // we need to delete the row for this queue name from the config table
    HTable hTable = new HTable(admin.getConfiguration(), configTableName);
    try {
      byte[] rowKey = queueName.toBytes();
      hTable.delete(new Delete(rowKey));
    } finally {
      hTable.close();
    }
  }

  private void deleteConsumerConfigurations(String app, String flow) throws IOException {
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

  public void create(QueueName queueName) throws IOException {
    String fullTableName = getActualTableName(queueName);

    // Queue Config needs to be on separate table, otherwise disabling the queue table would makes queue config
    // not accessible by the queue region coprocessor for doing eviction.
    byte[] tableNameBytes = Bytes.toBytes(fullTableName);
    byte[] configTableBytes = Bytes.toBytes(configTableName);

    // Create the config table first so that in case the queue table coprocessor runs, it can access the config table.
    HBaseTableUtil.createQueueTableIfNotExists(admin, configTableBytes, QueueEntryRow.COLUMN_FAMILY,
                                               QueueConstants.MAX_CREATE_TABLE_WAIT, 1, null);

    // Create the queue table with coprocessor
    Location jarDir = locationFactory.create(cConf.get(QueueConstants.ConfigKeys.QUEUE_TABLE_COPROCESSOR_DIR,
                                                       QueueConstants.DEFAULT_QUEUE_TABLE_COPROCESSOR_DIR));
    int splits = cConf.getInt(QueueConstants.ConfigKeys.QUEUE_TABLE_PRESPLITS,
                              QueueConstants.DEFAULT_QUEUE_TABLE_PRESPLITS);

    Class[] cps = getCoprocessors();
    String[] cpNames = new String[cps.length];
    for (int i = 0; i < cps.length; i++) {
      cpNames[i] = cps[i].getName();
    }

    HBaseTableUtil.createQueueTableIfNotExists(admin, tableNameBytes, QueueEntryRow.COLUMN_FAMILY,
                                               QueueConstants.MAX_CREATE_TABLE_WAIT, splits,
                                               HBaseTableUtil.createCoProcessorJar("queue", jarDir, cps),
                                               cpNames);
  }

  /**
   * @return coprocessors to set for the {@link HTable}
   */
  protected Class[] getCoprocessors() {
    return COPROCESSORS;
  }

  @Override
  public void dropAll() throws Exception {
    for (HTableDescriptor desc : admin.listTables()) {
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

    HTable hTable = new HTable(admin.getConfiguration(), configTableName);

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

    HTable hTable = new HTable(admin.getConfiguration(), configTableName);

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

    // If size increase, simply pick the lowest startRow from existing instances as startRow for the new instance
    if (instances > oldInstances) {
      // Set the startRow of the new instances to the smallest among existing
      Put put = new Put(rowKey);
      for (int i = oldInstances; i < instances; i++) {
        new HBaseConsumerState(smallest, groupId, i).updatePut(put);
      }
      mutations.add(put);

    } else {
      // If size decrease, reset all remaining instances startRow to min(smallest, startRow)
      // The map is sorted by instance ID
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
      if (!put.isEmpty()) {
        mutations.add(put);
      }
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
