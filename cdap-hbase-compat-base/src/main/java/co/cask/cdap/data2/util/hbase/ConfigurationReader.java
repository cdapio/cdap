/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Map;

/**
 * Reads the current {@link CConfiguration} from a table in HBase.  This make the configuration available
 * to processes running on the HBase servers (such as coprocessors).  The entire configuration is stored in
 * a single row, keyed by the configuration type (to allow future expansion), with the configuration
 * key as the column name, and the configuration value as the value.
 *
 * This class should only be used directly from the client side, and never within a coprocessor (use
 * {@link CConfigurationReader} instead when in a coprocessor.
 *
 * This class does not depend on any HBase Server classes and is safe to be used with only HBase Client
 * libraries in the class path.
 */
public class ConfigurationReader {
  /**
   * Defines the types of configurations to save in the table. Each type is used as a row key.
   */
  public enum Type {
    DEFAULT
  }

  // since this will be used in the coprocessor context, we use commons logging
  private static final Log LOG = LogFactory.getLog(ConfigurationReader.class);

  public static final String TABLE_NAME = "configuration";
  protected static final byte[] FAMILY = Bytes.toBytes("f");

  protected final ConfigurationTableProvider tableProvider;

  public ConfigurationReader(ConfigurationTableProvider tableProvider) {
    this.tableProvider = tableProvider;
  }

  /**
   * Use this constructor when not inside a coprocessor. This should only happen in:
   * <ul>
   *   <li>the Master, for writing the configuration</li>
   *   <li>test cases, for writing and reading/verifying</li>
   * </ul>
   */
  public ConfigurationReader(final Configuration hbaseConf, final CConfiguration cConf) {
    this(new ConfigurationTableProvider() {

      private final String tableName =
        HTableNameConverter.getSysConfigTablePrefix(cConf.get(Constants.Dataset.TABLE_PREFIX)) + TABLE_NAME;

      @Override
      public HTableInterface get() throws IOException {
        return new HTable(hbaseConf, tableName);
      }

      @Override
      public String getTableName() {
        return tableName;
      }
    });
  }

  /**
   * Reads the given configuration type from the HBase table, looking for the HBase table using CDAP's table prefix.
   *
   * @param type Type of configuration to read in
   * @return The {@link CConfiguration} instance populated with the stored values, or {@code null} if no row
   *         was found for the given type.
   * @throws IOException If an error occurs while attempting to read the table or the table does not exist.
   */
  public CConfiguration read(Type type) throws IOException {
    HTableInterface table = null;
    try {
      Get get = new Get(Bytes.toBytes(type.name()));
      get.addFamily(FAMILY);
      table = tableProvider.get();
      Result result = table.get(get);
      int propertyCount = 0;
      long timestamp = 0L;
      if (result != null && !result.isEmpty()) {
        CConfiguration conf = CConfiguration.create();
        conf.clear();
        Map<byte[], byte[]> kvs = result.getFamilyMap(FAMILY);
        for (Map.Entry<byte[], byte[]> e : kvs.entrySet()) {
          conf.set(Bytes.toString(e.getKey()), Bytes.toString(e.getValue()));
          propertyCount++;
          if (timestamp == 0L) {
            timestamp = result.getColumnLatestCell(FAMILY, e.getKey()).getTimestamp();
          }
        }
        LOG.info(String.format("Read %d properties with time stamp %d from row '%s' in configuration table %s ",
                               propertyCount, timestamp, type, table.getName()));
        return conf;
      } else {
        LOG.info(String.format("No configuration of type %s found in table %s.", type, table.getName()));
        return null;
      }
    } catch (TableNotFoundException e) {
      // this is expected because a coprocessor may start before the Master creates the table.
      // we log this here, because we have the fill table name; the caller does not need to log this again.
      LOG.info("Configuration table " + tableProvider.getTableName() + " does not yet exist");
      return null;
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException ioe) {
          LOG.warn("Error closing HTable for " + table.getName(), ioe);
        }
      }
    }
  }
}
