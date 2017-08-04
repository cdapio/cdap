/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.master.startup;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.startup.Check;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.data2.util.hbase.HBaseVersion;
import com.google.inject.Inject;
import com.google.inject.ProvisionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.tephra.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Checks that HBase is available.
 */
// class is picked up through classpath examination
@SuppressWarnings("unused")
class HBaseCheck extends Check {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseCheck.class);
  private final CConfiguration cConf;
  private final Configuration hConf;

  @Inject
  private HBaseCheck(CConfiguration cConf, Configuration hConf) {
    this.cConf = cConf;
    this.hConf = hConf;
  }

  @Override
  public void run() {
    LOG.info("Checking HBase version.");
    HBaseTableUtil hBaseTableUtil;
    try {
      hBaseTableUtil = new HBaseTableUtilFactory(cConf).get();
    } catch (ProvisionException e) {
      throw new RuntimeException("Unsupported Hbase version " + HBaseVersion.getVersionString());
    }
    LOG.info("  HBase version successfully verified.");

    LOG.info("Checking HBase availability.");

    try (Connection hbaseConnection = ConnectionFactory.createConnection(hConf)) {
      hbaseConnection.getAdmin().listTables();
      LOG.info("  HBase availability successfully verified.");
    } catch (IOException e) {
      throw new RuntimeException(
        "Unable to connect to HBase. " +
          "Please check that HBase is running and that the correct HBase configuration (hbase-site.xml) " +
          "and libraries are included in the CDAP master classpath.", e);
    }

    if (hConf.getBoolean("hbase.security.authorization", false)) {
      if (cConf.getBoolean(TxConstants.TransactionPruning.PRUNE_ENABLE)) {
        LOG.info("HBase authorization and transaction pruning are enabled. Checking global admin privileges for cdap.");
        try {
          boolean isGlobalAdmin = hBaseTableUtil.isGlobalAdmin(hConf);
          LOG.info("Global admin privileges check status: {}", isGlobalAdmin);
          if (isGlobalAdmin) {
            return;
          }
          // if global admin was false then depend on the TX_PRUNE_ACL_CHECK value
          if (cConf.getBoolean(Constants.Startup.TX_PRUNE_ACL_CHECK, false)) {
            LOG.info("Found {} to be set to true. Continuing with cdap master startup even though global admin check " +
                       "returned false", Constants.Startup.TX_PRUNE_ACL_CHECK);
            return;
          }
          StringBuilder builder = new StringBuilder("Transaction pruning is enabled and cdap does not have global " +
                                                      "admin privileges in HBase. Global admin privileges for cdap " +
                                                      "are required for transaction pruning. " +
                                                      "Either disable transaction pruning or grant global admin " +
                                                      "privilege to cdap in HBase or can override this " +
                                                      "check by setting ");
          builder.append(Constants.Startup.TX_PRUNE_ACL_CHECK);
          builder.append(" in cdap-site.xml.");
          if (HBaseVersion.get().equals(HBaseVersion.Version.HBASE_96) ||
            HBaseVersion.get().equals(HBaseVersion.Version.HBASE_98)) {
            builder.append(" Detected HBase version ");
            builder.append(HBaseVersion.get());
            builder.append(" CDAP will not be able determine if it has global admin privilege in HBase.");
            builder.append(" After granting global admin privilege please set ");
            builder.append(Constants.Startup.TX_PRUNE_ACL_CHECK);
          }
          throw new RuntimeException(builder.toString());
        } catch (IOException e) {
          throw new RuntimeException("Unable to determines cdap privileges as global admin in HBase.");
        }
      }
    }
    LOG.info("Hbase authorization is disabled. Skipping global admin check for transaction pruning.");
  }
}
