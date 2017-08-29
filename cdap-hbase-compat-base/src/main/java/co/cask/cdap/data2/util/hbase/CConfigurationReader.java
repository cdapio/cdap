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
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;

/**
 * This class helps abstract out the HBase version from reading of CConfiguration using {@link ConfigurationReader}.
 *
 * Use this class when within a coprocessor, to provide the configuration HTable through the coprocessor environment.
 */
public final class CConfigurationReader {

  private final ConfigurationReader configurationReader;

  public CConfigurationReader(final CoprocessorEnvironment env, final String tablePrefix) {
    this.configurationReader = new ConfigurationReader(new ConfigurationTableProvider() {

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

  /**
   * Use this constructor only in test cases. Within a coprocessor, the {@link CoprocessorEnvironment}
   * must be used to obtain the HBase table.
   */
  @VisibleForTesting
  public CConfigurationReader(Configuration hConf, CConfiguration cConf) {
    this.configurationReader = new ConfigurationReader(hConf, cConf);
  }

  public CConfiguration read() throws IOException {
    return configurationReader.read(ConfigurationReader.Type.DEFAULT);
  }
}
