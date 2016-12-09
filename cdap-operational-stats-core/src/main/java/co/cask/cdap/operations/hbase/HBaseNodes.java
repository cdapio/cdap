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

package co.cask.cdap.operations.hbase;

import co.cask.cdap.operations.OperationalStats;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * {@link OperationalStats} representing HBase node statistics.
 */
public class HBaseNodes extends AbstractHBaseStats implements HBaseNodesMXBean {

  private int masters;
  private int regionServers;
  private int deadRegionServers;

  @SuppressWarnings("unused")
  public HBaseNodes() {
    this(HBaseConfiguration.create());
  }

  @VisibleForTesting
  HBaseNodes(Configuration conf) {
    super(conf);
  }

  @Override
  public String getStatType() {
    return "nodes";
  }

  @Override
  public int getMasters() {
    return masters;
  }

  @Override
  public int getRegionServers() {
    return regionServers;
  }

  @Override
  public int getDeadRegionServers() {
    return deadRegionServers;
  }

  @Override
  public synchronized void collect() throws IOException {
    try (HBaseAdmin admin = new HBaseAdmin(conf)) {
      ClusterStatus clusterStatus = admin.getClusterStatus();
      // 1 master + number of backup masters
      masters = 1 + clusterStatus.getBackupMastersSize();
      regionServers = clusterStatus.getServersSize();
      deadRegionServers = clusterStatus.getDeadServers();
    }
  }
}
