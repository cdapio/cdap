/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.api.services.compute.Compute;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.GceClusterConfig;
import io.cdap.cdap.runtime.spi.provisioner.Node;

import java.util.Collections;

/**
 * Wrapper around the dataproc client that adheres to our configuration settings.
 */
class RuntimeMonitorDataprocClient extends DataprocClient {

  RuntimeMonitorDataprocClient(DataprocConf conf, ClusterControllerClient client, Compute compute) {
    super(conf, client, compute);
  }

  @Override
  protected Node getNode(Node.Type type, String zone, String nodeName) {
    return new Node(nodeName, type, null, 0, Collections.emptyMap());
  }

  @Override
  protected void setNetworkConfigs(GceClusterConfig.Builder clusterConfig, String network, boolean privateInstance) {
    String subnet = conf.getSubnet();
    // subnets are unique within a location, not within a network, which is why these configs are mutually exclusive.
    if (subnet != null) {
      clusterConfig.setSubnetworkUri(subnet);
    } else {
      clusterConfig.setNetworkUri(network);
    }

    //Add any defined Network Tags
    clusterConfig.addAllTags(conf.getNetworkTags());

    // if internal ip is preferred then create dataproc cluster without external ip for better security
    clusterConfig.setInternalIpOnly(privateInstance || !conf.isPreferExternalIP());
  }
}
