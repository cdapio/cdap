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
import co.cask.cdap.data2.transaction.snapshot.SnapshotCodecV1;
import co.cask.cdap.data2.transaction.snapshot.SnapshotCodecV2;
import co.cask.cdap.data2.util.hbase.ConfigurationTable;
import co.cask.tephra.coprocessor.TransactionStateCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

/**
 * Extends the {@link TransactionStateCache} implementation for
 * transaction coprocessors with a version that reads transaction configuration properties from
 * {@link ConfigurationTable}.  This allows the coprocessors to pick up configuration changes without requiring
 * a restart.
 */
public class DefaultTransactionStateCache extends TransactionStateCache {
  // CDAP versions of coprocessors must reference snapshot classes so they get included in generated jar file
  // DO NOT REMOVE
  private static final SnapshotCodecV1 codecV1 = null;
  private static final SnapshotCodecV2 codecV2 = null;

  private String sysConfigTablePrefix;
  private ConfigurationTable configTable;

  public DefaultTransactionStateCache(String sysConfigTablePrefix) {
    this.sysConfigTablePrefix = sysConfigTablePrefix;
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    this.configTable = new ConfigurationTable(conf);
  }

  @Override
  protected Configuration getSnapshotConfiguration() throws IOException {
    CConfiguration cConf = configTable.read(ConfigurationTable.Type.DEFAULT, sysConfigTablePrefix);
    Configuration txConf = HBaseConfiguration.create();
    CConfigurationUtil.copyTxProperties(cConf, txConf);
    return txConf;
  }
}
