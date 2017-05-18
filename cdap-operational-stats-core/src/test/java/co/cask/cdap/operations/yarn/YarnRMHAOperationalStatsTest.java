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

import co.cask.cdap.common.utils.Networks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;



/**
 * Tests operational stats with Resource Manager HA enabled
 */
public class YarnRMHAOperationalStatsTest extends AbstractYarnOperationalStatsTest {

  @Override
  protected MiniYARNCluster createYarnCluster() throws IOException, InterruptedException, YarnException {
    Configuration hConf = new Configuration();
    hConf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
    String hostname = MiniYARNCluster.getHostname();
    // Starts Zookeeper
    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    List<String> yarnKeys = new ArrayList<>();
    yarnKeys.add(YarnConfiguration.RM_ADDRESS);
    yarnKeys.add(YarnConfiguration.RM_SCHEDULER_ADDRESS);
    yarnKeys.add(YarnConfiguration.RM_ADMIN_ADDRESS);
    yarnKeys.add(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS);
    yarnKeys.add(YarnConfiguration.RM_WEBAPP_ADDRESS);

    for (String confKey : yarnKeys) {
      hConf.set(HAUtil.addSuffix(confKey, "rm0"), hostname + ":" + Networks.getRandomPort());
      hConf.set(HAUtil.addSuffix(confKey, "rm1"), hostname + ":" + Networks.getRandomPort());
    }

    hConf.set(YarnConfiguration.RM_CLUSTER_ID, "test-cluster");
    hConf.set(YarnConfiguration.RM_ZK_ADDRESS, zkServer.getConnectionStr());
    hConf.setInt(YarnConfiguration.RM_ZK_TIMEOUT_MS, 2000);

    MiniYARNCluster yarnCluster = new MiniYARNCluster(getClass().getName(), 2, 2, 2, 2);
    yarnCluster.init(hConf);
    yarnCluster.start();
    yarnCluster.getResourceManager(0).getRMContext().getRMAdminService().transitionToActive(
      new HAServiceProtocol.StateChangeRequestInfo(HAServiceProtocol.RequestSource.REQUEST_BY_USER));
    return yarnCluster;
  }

  @Override
  protected int getNumNodes() {
    return 2;
  }
}
