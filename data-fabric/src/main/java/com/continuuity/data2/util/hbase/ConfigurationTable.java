package com.continuuity.data2.util.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataSetAccessor;
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
    String tableName = getTableName(conf.get(DataSetAccessor.CFG_TABLE_PREFIX, DataSetAccessor.DEFAULT_TABLE_PREFIX));
    byte[] tableBytes = Bytes.toBytes(tableName);

    // must create the table if it doesn't exist
    HBaseAdmin admin = new HBaseAdmin(hbaseConf);
    HTable table = null;
    try {
      HTableDescriptor htd = new HTableDescriptor(tableBytes);
      htd.addFamily(new HColumnDescriptor(FAMILY));
      HBaseTableUtil tableUtil = new HBaseTableUtilFactory().get();
      tableUtil.createTableIfNotExists(admin, tableName, htd);

      long now = System.currentTimeMillis();
      long previous = now - 1;
      byte[] typeBytes = Bytes.toBytes(type.name());
      LOG.info("Writing new config row with key " + type);
      // populate the configuration data
      table = new HTable(hbaseConf, tableBytes);
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
        LOG.error("Error closing HBaseAdmin: " + ioe.getMessage(), ioe);
      }
      if (table != null) {
        try {
          table.close();
        } catch (IOException ioe) {
          LOG.error("Error closing HTable for " + tableName, ioe);
        }
      }
    }
  }

  /**
   * Reads the given configuration type from the HBase table, looking for the HBase table name under the
   * given "namespace".
   * @param type Type of configuration to read in
   * @param namespace Namespace to use in constructing the table name (should be the same as reactor.namespace)
   * @return The {@link CConfiguration} instance populated with the stored values, or {@code null} if no row
   *         was found for the given type.
   * @throws IOException If an error occurs while attempting to read the table or the table does not exist.
   */
  public CConfiguration read(Type type, String namespace) throws IOException {
    String tableName = getTableName(namespace);

    CConfiguration conf = null;
    HTable table = null;
    try {
      table = new HTable(hbaseConf, tableName);
      Get get = new Get(Bytes.toBytes(type.name()));
      get.addFamily(FAMILY);
      Result result = table.get(get);
      int propertyCnt = 0;
      if (result != null && !result.isEmpty()) {
        conf = new CConfiguration();
        Map<byte[], byte[]> kvs = result.getFamilyMap(FAMILY);
        for (Map.Entry<byte[], byte[]> e : kvs.entrySet()) {
          conf.set(Bytes.toString(e.getKey()), Bytes.toString(e.getValue()));
          propertyCnt++;
        }
      }
      LOG.info("Read " + propertyCnt + " properties from configuration table = " +
                 tableName + ", row = " + type.name());
    } catch (TableNotFoundException e) {
      // it's expected that this may occur when tables are created before ReactorServiceMain has started
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

  private static String getTableName(String reactorNamespace) {
    return reactorNamespace + "." + DataSetAccessor.Namespace.SYSTEM.namespace(TABLE_NAME);
  }
}
