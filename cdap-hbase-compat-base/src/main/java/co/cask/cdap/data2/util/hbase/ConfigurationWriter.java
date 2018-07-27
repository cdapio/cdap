/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Extends {@link ConfigurationReader} with the ability to write the configuration to HBase.
 *
 * This class should only be used from the client side, and never within a coprocessor (coprocessors
 * should only read, bit never write the configuration table).
 *
 * This class does not depend on any HBase Server classes and is safe to be used with only HBase Client
 * libraries in the class path.
 */
public class ConfigurationWriter extends ConfigurationReader {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationWriter.class);

  private final Configuration hConf;
  private final CConfiguration cConf;

  /**
   * Constructor from an HBase configuration and a CConfiguration.
   */
  public ConfigurationWriter(Configuration hConf, CConfiguration cConf) {
    super(hConf, cConf);
    this.cConf = cConf;
    this.hConf = hConf;
  }

  /**
   * Writes the {@link CConfiguration} instance as a new row to the HBase table.  The {@link Type} given is used as
   * the row key (allowing multiple configurations to be stored).  After the new configuration is written, this will
   * delete any configurations written with an earlier timestamp (to prevent removed values from being visible).
   *
   * @param configurationToWrite the CConfiguration to be persisted
   * @throws IOException If an error occurs while writing the configuration
   */
  public void write(Type type, CConfiguration configurationToWrite) throws IOException {

    createTableIfNecessary();

    long now = System.currentTimeMillis();
    // we use the type as the row key
    byte[] rowKey = Bytes.toBytes(type.name());
    // we will write the current config with the current timestamp
    Put p = new Put(rowKey);
    for (Map.Entry<String, String> e : configurationToWrite) {
      p.add(FAMILY, Bytes.toBytes(e.getKey()), now, Bytes.toBytes(e.getValue()));
    }
    // and we will delete any cells older than current time
    Delete d = new Delete(rowKey);
    d.deleteFamily(FAMILY, now - 1);

    HTableInterface table = tableProvider.get();
    try {
      table.setAutoFlushTo(false);
      LOG.info("Writing new configuration to row '{}' in configuration table {} and time stamp {}.",
               type, table.getName(), now);
      // populate the configuration data
      table.put(p);
      LOG.info("Deleting any configuration from row '{}' in configuration table {} with time stamp {} or older.",
               type, table.getName(), now - 1);
      table.delete(d);
    } finally {
      try {
        table.close();
      } catch (IOException ioe) {
        LOG.warn("Error closing HTable for {} ", table.getName(), ioe);
      }
    }
  }

  /**
   * Creates the configuration HBase table if it does not exist.
   */
  @VisibleForTesting
  void createTableIfNecessary() throws IOException {
    try (HBaseDDLExecutor ddlExecutor = new HBaseDDLExecutorFactory(cConf, hConf).get()) {
      HBaseTableUtil tableUtil = new HBaseTableUtilFactory(cConf).get();
      TableId tableId = tableUtil.createHTableId(NamespaceId.SYSTEM, TABLE_NAME);
      ColumnFamilyDescriptorBuilder cfdBuilder =
        HBaseTableUtil.getColumnFamilyDescriptorBuilder(Bytes.toString(FAMILY), hConf);
      TableDescriptorBuilder tdBuilder =
        HBaseTableUtil.getTableDescriptorBuilder(tableId, cConf).addColumnFamily(cfdBuilder.build());
      ddlExecutor.createTableIfNotExists(tdBuilder.build(), null);
    }
  }

  public static void upgradeTable(CConfiguration cConf, Configuration hConf) throws IOException {
    HBaseTableUtil tableUtil = new HBaseTableUtilFactory(cConf).get();
    TableId tableId = tableUtil.createHTableId(NamespaceId.SYSTEM, TABLE_NAME);

    try (HBaseDDLExecutor ddlExecutor = new HBaseDDLExecutorFactory(cConf, hConf).get()) {
      HTableDescriptor tableDescriptor;
      try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
        if (!tableUtil.tableExists(admin, tableId)) {
          LOG.debug("Table {} was not found. Skip upgrading.", tableId);
          return;
        }

        tableDescriptor = tableUtil.getHTableDescriptor(admin, tableId);
      }

      // Get cdap version from the table
      ProjectInfo.Version version = HBaseTableUtil.getVersion(tableDescriptor);
      String hbaseVersion = HBaseTableUtil.getHBaseVersion(tableDescriptor);

      if (hbaseVersion != null
        && hbaseVersion.equals(HBaseVersion.getVersionString())
        && version.compareTo(ProjectInfo.getVersion()) >= 0) {
        // If cdap has version has not changed or is greater, no need to update. Just enable it, in case
        // it has been disabled by the upgrade tool, and return
        LOG.info("Table '{}' has not changed and its version '{}' is same or greater than current CDAP version '{}'." +
                   " The underlying HBase version {} has also not changed.",
                 tableId, version, ProjectInfo.getVersion(), hbaseVersion);
        enableTable(ddlExecutor, tableId, cConf);
        return;
      }

      // create a new descriptor for the table update
      HTableDescriptorBuilder newDescriptor = tableUtil.buildHTableDescriptor(tableDescriptor);

      // Update CDAP version, table prefix
      HBaseTableUtil.setVersion(newDescriptor);
      HBaseTableUtil.setHBaseVersion(newDescriptor);
      HBaseTableUtil.setTablePrefix(newDescriptor, cConf);

      // Disable Table
      disableTable(ddlExecutor, tableId, cConf);

      tableUtil.modifyTable(ddlExecutor, newDescriptor.build());
      LOG.debug("Enabling table '{}'...", tableId);
      enableTable(ddlExecutor, tableId, cConf);
    }
    LOG.info("Table '{}' update completed.", tableId);
  }

  private static void enableTable(HBaseDDLExecutor ddlExecutor, TableId tableId,
                                  CConfiguration cConf) throws IOException {
    try {
      TableName tableName = HTableNameConverter.toTableName(cConf.get(Constants.Dataset.TABLE_PREFIX), tableId);
      ddlExecutor.enableTableIfDisabled(tableName.getNamespaceAsString(), tableName.getQualifierAsString());
      LOG.debug("Table {} has been enabled.", tableName);
    } catch (TableNotFoundException ex) {
      LOG.debug("Table {} was not found. Skipping enable.", tableId, ex);
    } catch (TableNotDisabledException ex) {
      LOG.debug("Table {} was already in enabled state.", tableId, ex);
    }
  }

  private static void disableTable(HBaseDDLExecutor ddlExecutor, TableId tableId,
                                   CConfiguration cConf) throws IOException {
    try {
      TableName tableName = HTableNameConverter.toTableName(cConf.get(Constants.Dataset.TABLE_PREFIX), tableId);
      ddlExecutor.disableTableIfEnabled(tableName.getNamespaceAsString(), tableName.getQualifierAsString());
      LOG.debug("Table {} has been disabled.", tableId);
    } catch (TableNotFoundException ex) {
      LOG.debug("Table {} was not found. Skipping disable.", tableId, ex);
    } catch (TableNotEnabledException ex) {
      LOG.debug("Table {} was already in disabled state.", tableId, ex);
    }
  }
}
