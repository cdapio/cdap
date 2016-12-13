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
 * {@link OperationalStats} representing current HBase load.
 */
public class HBaseLoad extends AbstractHBaseStats implements HBaseLoadMXBean {
  private int regions;
  private int regionsInTransition;
  private double averageLoad;
  private int requests;

  @SuppressWarnings("unused")
  public HBaseLoad() {
    this(HBaseConfiguration.create());
  }

  @VisibleForTesting
  HBaseLoad(Configuration conf) {
    super(conf);
  }

  @Override
  public String getStatType() {
    return "load";
  }

  @Override
  public int getTotalRegions() {
    return regions;
  }

  @Override
  public int getRegionsInTransition() {
    return regionsInTransition;
  }

  @Override
  public double getAverageRegionsPerServer() {
    return averageLoad;
  }

  @Override
  public int getNumRequests() {
    return requests;
  }

  @Override
  public void collect() throws IOException {
    try (HBaseAdmin admin = new HBaseAdmin(conf)) {
      ClusterStatus clusterStatus = admin.getClusterStatus();
      regions = clusterStatus.getRegionsCount();
      regionsInTransition = clusterStatus.getRegionsInTransition().size();
      averageLoad = clusterStatus.getAverageLoad();
      requests = clusterStatus.getRequestsCount();
    }
  }
}
