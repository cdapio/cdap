/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.coprocessor;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.CConfigurationUtil;
import co.cask.cdap.data2.util.hbase.CConfigurationReader;
import co.cask.cdap.data2.util.hbase.ConfigurationReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.coprocessor.TransactionStateCache;
import org.apache.tephra.snapshot.SnapshotCodecV3;

import java.io.IOException;

/**
 * Extends the {@link TransactionStateCache} implementation for
 * transaction coprocessors with a version that reads transaction configuration properties from
 * {@link ConfigurationReader}.  This allows the coprocessors to pick up configuration changes without requiring
 * a restart.
 */
public class DefaultTransactionStateCache extends TransactionStateCache {
  // CDAP versions of coprocessors must reference snapshot classes so they get included in generated jar file
  // DO NOT REMOVE
  @SuppressWarnings("unused")
  private static final SnapshotCodecV3 codecV3 = null;

  private String tablePrefix;
  private CConfigurationReader configReader;
  private final CoprocessorEnvironment env;

  public DefaultTransactionStateCache(String tablePrefix, CoprocessorEnvironment env) {
    this.tablePrefix = tablePrefix;
    this.env = env;
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    this.configReader = new CConfigurationReader(env, tablePrefix);
  }

  @Override
  protected Configuration getSnapshotConfiguration() throws IOException {
    CConfiguration cConf = configReader.read();
    Configuration txConf = HBaseConfiguration.create(getConf());
    CConfigurationUtil.copyTxProperties(cConf, txConf);
    return txConf;
  }
}
