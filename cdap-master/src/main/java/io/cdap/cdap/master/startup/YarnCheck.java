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
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Checks that YARN is available and has enough resources to run all system services.
 */
// class is picked up through classpath examination
@SuppressWarnings("unused")
class YarnCheck extends AbstractMasterCheck {
  private static final Logger LOG = LoggerFactory.getLogger(YarnCheck.class);
  private final Configuration hConf;

  @Inject
  private YarnCheck(CConfiguration cConf, Configuration hConf) {
    super(cConf);
    this.hConf = hConf;
  }

  @Override
  public void run() {
    int yarnConnectTimeout = cConf.getInt(Constants.Startup.YARN_CONNECT_TIMEOUT_SECONDS, 60);
    LOG.info("Checking YARN availability -- may take up to {} seconds.", yarnConnectTimeout);

    final YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(hConf);

    List<NodeReport> nodeReports;
    // if yarn is not up, yarnClient.start() will hang.
    ExecutorService executorService = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setNameFormat("startup-checker").build());
    try {
      Future<List<NodeReport>> result = executorService.submit(new Callable<List<NodeReport>>() {
        @Override
        public List<NodeReport> call() throws Exception {
          yarnClient.start();
          return yarnClient.getNodeReports();
        }
      });
      nodeReports = result.get(yarnConnectTimeout, TimeUnit.SECONDS);
      LOG.info("  YARN availability successfully verified.");
    } catch (Exception e) {
      throw new RuntimeException(
        "Unable to get status of YARN nodemanagers. " +
          "Please check that YARN is running " +
          "and that the correct Hadoop configuration (core-site.xml, yarn-site.xml) and libraries " +
          "are included in the CDAP master classpath.", e);
    } finally {
      try {
        yarnClient.stop();
      } catch (Exception e) {
        LOG.warn("Error stopping yarn client.", e);
      } finally {
        executorService.shutdown();
      }
    }

    checkResources(nodeReports);
  }

  private void checkResources(List<NodeReport> nodeReports) {
    LOG.info("Checking that YARN has enough resources to run all system services.");
    int memoryCapacity = 0;
    int vcoresCapacity = 0;
    int memoryUsed = 0;
    int vcoresUsed = 0;
    int availableNodes = 0;
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
          memoryCapacity += nodeCapability.getMemory();
          vcoresCapacity += nodeCapability.getVirtualCores();
        }

        if (nodeUsed != null) {
          LOG.debug("node {} resources used: memory = {}, vcores = {}", nodeId,
                    nodeUsed.getMemory(), nodeUsed.getVirtualCores());
          memoryUsed += nodeUsed.getMemory();
          vcoresUsed += nodeUsed.getVirtualCores();
        }

        availableNodes++;
      }
    }
    LOG.debug("YARN resource capacity: {} MB of memory and {} virtual cores.", memoryCapacity, vcoresCapacity);
    LOG.debug("YARN resources used: {} MB of memory and {} virtual cores.", memoryUsed, vcoresUsed);

    // calculate memory and vcores required by CDAP
    int requiredMemoryMB = 0;
    int requiredVCores = 0;
    Set<String> invalidKeys = new HashSet<>();
    for (ServiceResourceKeys serviceResourceKeys : systemServicesResourceKeys) {
      boolean hasConfigError = false;
      int instances = 0;
      int memoryMB = 0;
      int vcores = 0;

      try {
        instances = serviceResourceKeys.getInstances();
      } catch (Exception e) {
        invalidKeys.add(serviceResourceKeys.getInstancesKey());
        hasConfigError = true;
      }
      try {
        memoryMB = serviceResourceKeys.getMemory();
      } catch (Exception e) {
        invalidKeys.add(serviceResourceKeys.getMemoryKey());
        hasConfigError = true;
      }
      try {
        vcores = serviceResourceKeys.getVcores();
      } catch (Exception e) {
        invalidKeys.add(serviceResourceKeys.getVcoresKey());
        hasConfigError = true;
      }

      if (!hasConfigError) {
        LOG.debug("Resource settings for system service {}: {}={}, {}={}, {}={}",
                  serviceResourceKeys.getServiceName(),
                  serviceResourceKeys.getInstancesKey(), instances,
                  serviceResourceKeys.getMemoryKey(), memoryMB,
                  serviceResourceKeys.getVcoresKey(), vcores);
        requiredMemoryMB += memoryMB * instances;
        requiredVCores += vcores * instances;
      }
    }

    if (!invalidKeys.isEmpty()) {
      throw new RuntimeException(
        "YARN resources check failed to invalid config settings for keys: " + Joiner.on(',').join(invalidKeys));
    }

    LOG.debug("{} MB of memory and {} virtual cores are required.", requiredMemoryMB, requiredVCores);

    checkResources(requiredMemoryMB, requiredVCores, memoryCapacity, vcoresCapacity, "in capacity");

    int availableMemoryMB = memoryCapacity - memoryUsed;
    int availableVCores = vcoresCapacity - vcoresUsed;
    try {
      checkResources(requiredMemoryMB, requiredVCores, availableMemoryMB, availableVCores, "available");
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }

    LOG.info("  YARN resources successfully verified.");
  }

  private void checkResources(int requiredMemoryMB, int requiredVCores,
                              int actualMemoryMB, int actualVCores,
                              String errorSuffix) {
    boolean memoryBad = requiredMemoryMB > actualMemoryMB;
    boolean vcoresBad = requiredVCores > actualVCores;

    if (memoryBad && vcoresBad) {
      throw new RuntimeException(String.format(
        "Services require %d MB of memory and %d vcores, but the cluster only has %d memory and %d vcores %s.",
        requiredMemoryMB, requiredVCores, actualMemoryMB, actualVCores, errorSuffix));
    } else if (memoryBad) {
      throw new RuntimeException(String.format(
        "Services require %d MB of memory but the cluster only has %d MB of memory %s.",
        requiredMemoryMB, actualMemoryMB, errorSuffix));
    } else if (vcoresBad) {
      throw new RuntimeException(String.format(
        "Services require %d vcores but the cluster only has %d vcores %s.",
        requiredVCores, actualVCores, errorSuffix));
    }
  }
}
