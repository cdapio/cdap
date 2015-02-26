/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.transaction.queue.AbstractQueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.hbase.wd.AbstractRowKeyDistributor;
import co.cask.cdap.hbase.wd.RowKeyDistributorByHashPrefix;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static co.cask.cdap.data2.transaction.queue.QueueConstants.QueueType.QUEUE;

/**
 * admin for queues in hbase.
 */
@Singleton
public class HBaseQueueAdmin extends AbstractQueueAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueueAdmin.class);

  /**
   * HBase table property for the number of bytes as the prefix of the queue entry row key.
   */
  public static final String PROPERTY_PREFIX_BYTES = "cdap.prefix.bytes";

  public static final int SALT_BYTES = 1;
  public static final int ROW_KEY_DISTRIBUTION_BUCKETS = 8;
  public static final AbstractRowKeyDistributor ROW_KEY_DISTRIBUTOR =
    new RowKeyDistributorByHashPrefix(
      new RowKeyDistributorByHashPrefix.OneByteSimpleHash(ROW_KEY_DISTRIBUTION_BUCKETS));

  protected final HBaseTableUtil tableUtil;
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final QueueConstants.QueueType type;

  private HBaseAdmin admin;

  @Inject
  public HBaseQueueAdmin(Configuration hConf,
                         CConfiguration cConf,
                         LocationFactory locationFactory,
                         HBaseTableUtil tableUtil) throws IOException {
    this(hConf, cConf, locationFactory, tableUtil, QUEUE);
  }

  protected HBaseQueueAdmin(Configuration hConf,
                            CConfiguration cConf,
                            LocationFactory locationFactory,
                            HBaseTableUtil tableUtil,
                            QueueConstants.QueueType type) throws IOException {
    super(cConf, type);
    this.hConf = hConf;
    this.cConf = cConf;
    this.tableUtil = tableUtil;
    this.type = type;
    this.locationFactory = locationFactory;
  }

  protected final synchronized HBaseAdmin getHBaseAdmin() throws IOException {
    if (admin == null) {
      admin = new HBaseAdmin(hConf);
    }
    return admin;
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

  @Override
  public boolean exists(String name) throws Exception {
    return exists(QueueName.from(URI.create(name)));
  }

  boolean exists(QueueName queueName) throws IOException {
    HBaseAdmin admin = getHBaseAdmin();
    return admin.tableExists(getActualTableName(queueName)) && admin.tableExists(getConfigTableName(queueName));
  }

  @Override
  public void create(String name, Properties props) throws Exception {
    create(QueueName.from(URI.create(name)), props);
  }

  @Override
  public void create(String name) throws Exception {
    create(name, new Properties());
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
  public void clearAllForFlow(String namespaceId, String app, String flow) throws Exception {
    // all queues for a flow are in one table
    String tableName = getTableNameForFlow(namespaceId, app, flow);
    truncate(Bytes.toBytes(tableName));
    // we also have to delete the config for these queues
    deleteConsumerConfigurations(namespaceId, app, flow);
  }

  @Override
  public void dropAllForFlow(String namespaceId, String app, String flow) throws Exception {
    // all queues for a flow are in one table
    String tableName = getTableNameForFlow(namespaceId, app, flow);
    drop(Bytes.toBytes(tableName));
    // we also have to delete the config for these queues
    deleteConsumerConfigurations(namespaceId, app, flow);
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

  @Override
  public void upgrade(String tableName, Properties properties) throws Exception {
    AbstractHBaseDataSetAdmin dsAdmin = new DatasetAdmin(tableName, hConf, tableUtil, properties);
    try {
      dsAdmin.upgrade();
    } finally {
      dsAdmin.close();
    }
  }

  protected HTable createConfigHTable(String configTableName) throws IOException {
    HTable hTable = new HTable(admin.getConfiguration(), configTableName);
    hTable.setAutoFlush(false);
    return hTable;
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
    HTable hTable = new HTable(getHBaseAdmin().getConfiguration(), getConfigTableName(queueName));
    try {
      byte[] rowKey = queueName.toBytes();
      hTable.delete(new Delete(rowKey));
    } finally {
      hTable.close();
    }
  }

  private void deleteConsumerConfigurations(String namespaceId, String app, String flow) throws IOException {
    deleteConsumerConfigurationsForPrefix(QueueName.prefixForFlow(namespaceId, app, flow),
                                          getConfigTableName(namespaceId));
  }

  /**
   * @param rowPrefix defines the entries to be removed
   * @param configTableName the config table to remove the configurations from
   * @throws IOException
   */
  private void deleteConsumerConfigurationsForPrefix(String rowPrefix, String configTableName)
    throws IOException {
    // table is created lazily, possible it may not exist yet.
    HBaseAdmin admin = getHBaseAdmin();
    if (admin.tableExists(configTableName)) {
      // we need to delete the row for this queue name from the config table
      HTable hTable = createConfigHTable(configTableName);
      try {
        byte[] prefix = Bytes.toBytes(rowPrefix);
        byte[] stop = Bytes.stopKeyForPrefix(prefix);

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
        hTable.flushCommits();

      } finally {
        hTable.close();
      }
    }
  }

  /**
   * Creates the HBase table for the given queue with empty properties.
   *
   * @see #create(QueueName, Properties)
   */
  public void create(QueueName queueName) throws IOException {
    create(queueName, new Properties());
  }

  /**
   * Creates the HBase table for th given queue and set the given properties into the table descriptor.
   *
   * @param queueName Name of the queue.
   * @param properties Set of properties to store in the table.
   */
  public void create(QueueName queueName, Properties properties) throws IOException {
    // Queue Config needs to be on separate table, otherwise disabling the queue table would makes queue config
    // not accessible by the queue region coprocessor for doing eviction.

    // Create the config table first so that in case the queue table coprocessor runs, it can access the config table.
    createConfigTable(getConfigTableName(queueName));

    String hBaseTableName = getActualTableName(queueName);
    DatasetAdmin dsAdmin = new DatasetAdmin(hBaseTableName, hConf, tableUtil, properties);
    try {
      dsAdmin.create();
    } finally {
      dsAdmin.close();
    }
  }

  private void createConfigTable(String configTableName) throws IOException {
    byte[] tableName = Bytes.toBytes(configTableName);
    HTableDescriptor htd = new HTableDescriptor(tableName);

    HColumnDescriptor hcd = new HColumnDescriptor(QueueEntryRow.COLUMN_FAMILY);
    hcd.setMaxVersions(1);
    htd.addFamily(hcd);

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
  public void dropAllInNamespace(String namespaceId) throws Exception {
    // Note: The trailing "." is crucial, since otherwise nsId could match nsId1, nsIdx etc
    dropTablesWithPrefix(String.format("%s.", getTableNamePrefix(namespaceId)));
    drop(Bytes.toBytes(getConfigTableName(namespaceId)));
  }

  @Override
  public void configureInstances(QueueName queueName, long groupId, int instances) throws Exception {
    Preconditions.checkArgument(instances > 0, "Number of consumer instances must be > 0.");

    if (!exists(queueName)) {
      create(queueName);
    }

    HBaseConsumerStateStore stateStore = createConsumerStateStore(queueName);
    try {
      stateStore.configureInstances(groupId, instances);
    } finally {
      stateStore.close();
    }
  }

  @Override
  public void configureGroups(QueueName queueName, Map<Long, Integer> groupInfo) throws Exception {
    Preconditions.checkArgument(!groupInfo.isEmpty(), "Consumer group information must not be empty.");

    if (!exists(queueName)) {
      create(queueName);
    }

    HBaseConsumerStateStore stateStore = createConsumerStateStore(queueName);
    try {
      stateStore.configureGroups(groupInfo);
    } finally {
      stateStore.close();
    }
  }

  @Override
  public void upgrade() throws Exception {
    // For each table managed by this admin, performs an upgrade
    for (HTableDescriptor desc : getHBaseAdmin().listTables()) {
      String tableName = Bytes.toString(desc.getName());
      // It's important to skip config table enabled.
      if (isDataTable(tableName)) {
        LOG.info(String.format("Upgrading queue hbase table: %s", tableName));
        Properties properties = new Properties();
        if (desc.getValue(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES) == null) {
          // It's the old queue table. Set the property prefix bytes to SALT_BYTES
          properties.setProperty(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES, Integer.toString(HBaseQueueAdmin.SALT_BYTES));
        }
        upgrade(tableName, properties);
        LOG.info(String.format("Upgraded queue hbase table: %s", tableName));
      }
    }
  }

  @VisibleForTesting
  HBaseConsumerStateStore createConsumerStateStore(QueueName queueName) throws IOException {
    HTable hTable = createConfigHTable(getConfigTableName(queueName));
    return new HBaseConsumerStateStore(queueName, hTable);
  }

  /**
   * @param tableName being checked
   * @return true if the given table is the actual table for the queue (opposed to the config table for the queue
   * or tables for things other than queues).
   */
  private boolean isDataTable(String tableName) {
    // checks if table is constructed by getActualTableName(String)
    String[] parts = tableName.split("\\.");
    if (parts.length != 6) {
      return false;
    }
    String namespace = parts[1];
    String tableNamePrefix = getTableNamePrefix(namespace);
    return tableName.startsWith(tableNamePrefix);
  }

  private void dropTablesWithPrefix(String tableNamePrefix) throws IOException {
    for (HTableDescriptor desc : getHBaseAdmin().listTables()) {
      String tableName = Bytes.toString(desc.getName());
      // It's important to keep config table enabled while disabling queue tables.
      if (tableName.startsWith(tableNamePrefix) && !isConfigTable(tableName)) {
        drop(desc.getName());
      }
    }
  }

  private boolean isConfigTable(String tableName) {
    String[] parts = tableName.split("\\.");
    if (parts.length != 5) {
      return false;
    }
    String namespace = parts[1];
    String tableNamePrefix = getConfigTableName(namespace);
    return tableName.startsWith(tableNamePrefix);
  }

  protected void createQueueTable(HTableDescriptor htd, byte[][] splitKeys) throws IOException {
    if (htd.getValue(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES) == null) {
      htd.setValue(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES, Integer.toString(HBaseQueueAdmin.SALT_BYTES));
    }
    LOG.info("Create queue table with prefix bytes {}", htd.getValue(HBaseQueueAdmin.PROPERTY_PREFIX_BYTES));
    tableUtil.createTableIfNotExists(getHBaseAdmin(), htd.getName(), htd, splitKeys);
  }

  // only used for create & upgrade of data table
  private final class DatasetAdmin extends AbstractHBaseDataSetAdmin {

    private final Properties properties;

    private DatasetAdmin(String name, Configuration hConf, HBaseTableUtil tableUtil, Properties properties) {
      super(name, hConf, tableUtil);
      this.properties = properties;
    }

    @Override
    protected CoprocessorJar createCoprocessorJar() throws IOException {
      List<? extends Class<? extends Coprocessor>> coprocessors = getCoprocessors();
      if (coprocessors.isEmpty()) {
        return CoprocessorJar.EMPTY;
      }

      Location jarDir = locationFactory.create(cConf.get(QueueConstants.ConfigKeys.QUEUE_TABLE_COPROCESSOR_DIR,
                                                         QueueConstants.DEFAULT_QUEUE_TABLE_COPROCESSOR_DIR));
      Location jarFile = HBaseTableUtil.createCoProcessorJar(type.toString(), jarDir, coprocessors);
      return new CoprocessorJar(coprocessors, jarFile);
    }

    @Override
    protected boolean upgradeTable(HTableDescriptor tableDescriptor) {
      boolean updated = false;
      HColumnDescriptor columnDescriptor = tableDescriptor.getFamily(QueueEntryRow.COLUMN_FAMILY);
      if (columnDescriptor.getMaxVersions() != 1) {
        columnDescriptor.setMaxVersions(1);
        updated = true;
      }
      for (String key : properties.stringPropertyNames()) {
        String oldValue = tableDescriptor.getValue(key);
        String newValue = properties.getProperty(key);
        if (!Objects.equal(oldValue, newValue)) {
          tableDescriptor.setValue(key, newValue);
          updated = true;
        }
      }
      return updated;
    }

    @Override
    public void create() throws IOException {
      // Create the queue table
      byte[] tableName = Bytes.toBytes(this.tableName);
      HTableDescriptor htd = new HTableDescriptor(tableName);
      for (String key : properties.stringPropertyNames()) {
        htd.setValue(key, properties.getProperty(key));
      }

      HColumnDescriptor hcd = new HColumnDescriptor(QueueEntryRow.COLUMN_FAMILY);
      hcd.setMaxVersions(1);
      htd.addFamily(hcd);

      // Add coprocessors
      CoprocessorJar coprocessorJar = createCoprocessorJar();
      for (Class<? extends Coprocessor> coprocessor : coprocessorJar.getCoprocessors()) {
        addCoprocessor(htd, coprocessor, coprocessorJar.getJarLocation(), coprocessorJar.getPriority(coprocessor));
      }

      // Create queue table with splits.
      int splits = cConf.getInt(QueueConstants.ConfigKeys.QUEUE_TABLE_PRESPLITS,
                                QueueConstants.DEFAULT_QUEUE_TABLE_PRESPLITS);
      byte[][] splitKeys = HBaseTableUtil.getSplitKeys(splits);
      createQueueTable(htd, splitKeys);
    }
  }
}
