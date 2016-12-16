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

package co.cask.cdap.operations.yarn;

import co.cask.cdap.operations.OperationalStats;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.YarnClient;

import java.util.List;

/**
 * {@link OperationalStats} for reporting Yarn node stats.
 */
public class YarnNodes extends AbstractYarnStats implements YarnNodesMXBean {

  private int healthyNodes;
  private int newNodes;
  private int unusableNodes;
  private int healthyContainers;
  private int newContainers;
  private int unusableContainers;

  @SuppressWarnings("unused")
  public YarnNodes() {
    this(new Configuration());
  }

  @VisibleForTesting
  YarnNodes(Configuration conf) {
    super(conf);
  }

  @Override
  public String getStatType() {
    return "nodes";
  }

  @Override
  public int getTotalNodes() {
    return healthyNodes + newNodes + unusableNodes;
  }

  @Override
  public int getHealthyNodes() {
    return healthyNodes;
  }

  @Override
  public int getNewNodes() {
    return newNodes;
  }

  @Override
  public int getUnusableNodes() {
    return unusableNodes;
  }

  @Override
  public int getTotalContainers() {
    return healthyContainers + newContainers + unusableContainers;
  }

  @Override
  public int getHealthyContainers() {
    return healthyContainers;
  }

  @Override
  public int getNewContainers() {
    return newContainers;
  }

  @Override
  public int getUnusableContainers() {
    return unusableContainers;
  }

  @Override
  public synchronized void collect() throws Exception {
    reset();
    List<NodeReport> nodeReports;
    YarnClient yarnClient = createYARNClient();
    try {
      nodeReports = yarnClient.getNodeReports();
    } finally {
      yarnClient.stop();
    }
    for (NodeReport nodeReport : nodeReports) {
      switch (nodeReport.getNodeState()) {
        case RUNNING:
          healthyNodes++;
          healthyContainers += nodeReport.getNumContainers();
          break;
        case UNHEALTHY:
        case DECOMMISSIONED:
        case LOST:
          unusableNodes++;
          unusableContainers += nodeReport.getNumContainers();
          break;
        case NEW:
        case REBOOTED:
          newNodes++;
          newContainers += nodeReport.getNumContainers();
          break;
      }
    }
  }

  private void reset() {
    healthyNodes = 0;
    unusableNodes = 0;
    newNodes = 0;
    healthyContainers = 0;
    unusableContainers = 0;
    newContainers = 0;
  }
}
