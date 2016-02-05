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

import co.cask.cdap.common.startup.Check;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
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
  private final Configuration hConf;

  @Inject
  private HBaseCheck(Configuration hConf) {
    this.hConf = hConf;
  }

  @Override
  public void run() {
    LOG.info("Checking HBase availability.");
    try (final HConnection hbaseConnection = HConnectionManager.createConnection(hConf)) {
      hbaseConnection.listTables();
      LOG.info("  HBase availability successfully verified.");
    } catch (IOException e) {
      throw new RuntimeException(
        "Unable to connect to HBase. " +
          "Please check that HBase is running and that the correct HBase configuration (hbase-site.xml) " +
          "and libraries are included in the CDAP master classpath.", e);
    }
  }
}
