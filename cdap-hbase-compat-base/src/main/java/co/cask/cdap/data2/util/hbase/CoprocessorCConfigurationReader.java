/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;

/**
 * This class implements the reading of the {@link CConfiguration} from HBase, when inside a coprocessor.
 */
public final class CoprocessorCConfigurationReader extends ConfigurationReader implements CConfigurationReader {

  /**
   * Constructor using the coprocessor environment. In order to construct the correct table name,
   * the CDAP table namespace prefix must be provided. This is configured in the {@link CConfiguration}
   * as well as an attribute for the HBase tables created by CDAP; in both cases with the key
   * {@link Constants.Dataset#TABLE_PREFIX}.
   *
   * @param env the coprocessor environment
   * @param tablePrefix the namespace prefix used for CDAP tables
   */
  public CoprocessorCConfigurationReader(final CoprocessorEnvironment env, final String tablePrefix) {
    super(new ConfigurationTableProvider() {

      private final TableName tableName =
        TableName.valueOf(HTableNameConverter.getSystemNamespace(tablePrefix), ConfigurationReader.TABLE_NAME);

      @Override
      public HTableInterface get() throws IOException {
        return env.getTable(tableName);
      }

      @Override
      public String getTableName() {
        return tableName.toString();
      }
    });
  }

  @Override
  public CConfiguration read() throws IOException {
    return read(Type.DEFAULT);
  }
}
