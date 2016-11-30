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
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * {@link OperationalStats} for reporting Yarn resources.
 */
public class YarnResources extends AbstractYarnStats implements YarnResourcesMXBean {
  private static final Logger LOG = LoggerFactory.getLogger(YarnResources.class);

  private int totalMemory;
  private int usedMemory;
  private int totalVCores;
  private int usedVCores;

  @SuppressWarnings("unused")
  public YarnResources() {
    this(new Configuration());
  }

  @VisibleForTesting
  YarnResources(Configuration conf) {
    super(conf);
  }

  @Override
  public String getStatType() {
    return "resources";
  }

  @Override
  public int getTotalMemory() {
    return totalMemory;
  }

  @Override
  public int getUsedMemory() {
    return usedMemory;
  }

  @Override
  public int getFreeMemory() {
    return totalMemory - usedMemory;
  }

  @Override
  public int getTotalVCores() {
    return totalVCores;
  }

  @Override
  public int getUsedVCores() {
    return usedVCores;
  }

  @Override
  public int getFreeVCores() {
    return totalVCores - usedVCores;
  }

  @Override
  public synchronized void collect() throws IOException {
    reset();
    List<NodeReport> nodeReports;
    YarnClient yarnClient = createYARNClient();
    try {
      nodeReports = yarnClient.getNodeReports();
    } catch (YarnException e) {
      throw new IOException(e);
    } finally {
      yarnClient.stop();
    }
    for (NodeReport nodeReport : nodeReports) {
      NodeId nodeId = nodeReport.getNodeId();
      LOG.debug("Got report for node {}", nodeId);
      if (!nodeReport.getNodeState().isUnusable()) {
        Resource nodeCapability = nodeReport.getCapability();
        Resource nodeUsed = nodeReport.getUsed();

        // some versions of hadoop return null, others do not
        if (nodeCapability != null) {
          LOG.debug("node {} resource capability: memory = {}, vcores = {}", nodeId,
                    nodeCapability.getMemory(), nodeCapability.getVirtualCores());
          totalMemory += nodeCapability.getMemory();
          totalVCores += nodeCapability.getVirtualCores();
        }

        if (nodeUsed != null) {
          LOG.debug("node {} resources used: memory = {}, vcores = {}", nodeId,
                    nodeUsed.getMemory(), nodeUsed.getVirtualCores());
          usedMemory += nodeUsed.getMemory();
          usedVCores += nodeUsed.getVirtualCores();
        }
      }
    }
  }

  private void reset() {
    totalMemory = 0;
    usedMemory = 0;
    totalVCores = 0;
    usedVCores = 0;
  }
}
