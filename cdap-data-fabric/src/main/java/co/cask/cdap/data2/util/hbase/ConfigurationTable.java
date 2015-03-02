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

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Map;

/**
 * Reads and writes current {@link CConfiguration} to a table in HBase.  This make the configuration available
 * to processes running on the HBase servers (such as coprocessors).  The entire configuration is stored in
 * a single row, keyed by the configuration type (to allow future expansion), with the configuration
 * key as the column name, and the configuration value as the value.
 */
public class ConfigurationTable {
  /**
   * Defines the types of configurations to save in the table.  Each type is used as a row key.
   */
  public enum Type {
    DEFAULT
  }

  // since this will be used in the coprocessor context, we use commons logging
  private static final Log LOG = LogFactory.getLog(ConfigurationTable.class);

  private static final String TABLE_NAME = "configuration";
  private static final byte[] FAMILY = Bytes.toBytes("f");

  private final Configuration hbaseConf;

  public ConfigurationTable(Configuration hbaseConf) {
    this.hbaseConf = hbaseConf;
  }

  /**
   * Writes the {@link CConfiguration} instance as a new row to the HBase table.  The {@link Type} given is used as
   * the row key (allowing multiple configurations to be stored).  After the new configuration is written, this will
   * delete any configurations written with an earlier timestamp (to prevent removed values from being visible).
   * @param conf The CConfiguration instance to store
   * @throws IOException If an error occurs while writing the configuration
   */
  public void write(Type type, CConfiguration conf) throws IOException {
    TableId tableId = TableId.from(conf.get(Constants.Dataset.TABLE_PREFIX), Constants.SYSTEM_NAMESPACE, TABLE_NAME);
    // must create the table if it doesn't exist
    HBaseAdmin admin = new HBaseAdmin(hbaseConf);
    HTable table = null;
    try {
      HBaseTableUtil tableUtil = new HBaseTableUtilFactory().get();
      HTableDescriptor htd = tableUtil.getHTableDescriptor(tableId);
      htd.addFamily(new HColumnDescriptor(FAMILY));
      tableUtil.createTableIfNotExists(admin, tableId, htd);

      long now = System.currentTimeMillis();
      long previous = now - 1;
      byte[] typeBytes = Bytes.toBytes(type.name());
      LOG.info("Writing new config row with key " + type);
      // populate the configuration data
      table = tableUtil.getHTable(hbaseConf, tableId);
      table.setAutoFlush(false);
      Put p = new Put(typeBytes);
      for (Map.Entry<String, String> e : conf) {
        p.add(FAMILY, Bytes.toBytes(e.getKey()), now, Bytes.toBytes(e.getValue()));
      }
      table.put(p);

      LOG.info("Deleting any configuration from " + previous + " or before");
      Delete d = new Delete(typeBytes);
      d.deleteFamily(FAMILY, previous);
      table.delete(d);
    } finally {
      try {
        admin.close();
      } catch (IOException ioe) {
        LOG.error("Error closing HBaseAdmin: ", ioe);
      }
      if (table != null) {
        try {
          table.close();
        } catch (IOException ioe) {
          LOG.error("Error closing HBaseAdmin: " + ioe.getMessage(), ioe);
        }
      }
    }
  }

  /**
   * Reads the given configuration type from the HBase table, looking for the HBase table name under the
   * given "sysConfigTablePrefix".
   * @param type Type of configuration to read in
   * @param sysConfigTablePrefix table prefix of the configuration table. (The full table name of the configuration
   *                             table minus the table qualifier). Example: 'cdap.system:'
   * @return The {@link CConfiguration} instance populated with the stored values, or {@code null} if no row
   *         was found for the given type.
   * @throws IOException If an error occurs while attempting to read the table or the table does not exist.
   */
  public CConfiguration read(Type type, String sysConfigTablePrefix) throws IOException {
    String tableName = sysConfigTablePrefix + TABLE_NAME;
    HTable table = null;
    CConfiguration conf = null;
    try {
      table = new HTable(hbaseConf, tableName);
      Get get = new Get(Bytes.toBytes(type.name()));
      get.addFamily(FAMILY);
      Result result = table.get(get);
      int propertyCnt = 0;
      if (result != null && !result.isEmpty()) {
        conf = CConfiguration.create();
        conf.clear();
        Map<byte[], byte[]> kvs = result.getFamilyMap(FAMILY);
        for (Map.Entry<byte[], byte[]> e : kvs.entrySet()) {
          conf.set(Bytes.toString(e.getKey()), Bytes.toString(e.getValue()));
          propertyCnt++;
        }
      }
      LOG.info("Read " + propertyCnt + " properties from configuration table = " +
                 Bytes.toString(table.getTableName()) + ", row = " + type.name());
    } catch (TableNotFoundException e) {
      // it's expected that this may occur when tables are created before MasterServiceMain has started
      LOG.warn("Configuration table " + tableName + " does not yet exist.");
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException ioe) {
          LOG.error("Error closing HTable for " + tableName, ioe);
        }
      }
    }
    return conf;
  }
}
