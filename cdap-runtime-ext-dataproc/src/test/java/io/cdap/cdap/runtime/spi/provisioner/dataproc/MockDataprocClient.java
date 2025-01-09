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
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.runtime.spi.provisioner.Node;
import java.util.Collections;

public class MockDataprocClient extends DataprocClient {
  public MockDataprocClient(DataprocConf conf, ClusterControllerClient client,
      ComputeFactory computeFactory,ErrorCategory errorCategory) {
    super(conf, client, computeFactory, errorCategory);
  }

  @Override
  protected void setNetworkConfigs(Compute compute, GceClusterConfig.Builder clusterConfig,
                                   boolean privateInstance) {
    // no-op
  }

  @Override
  protected Node getNode(Node.Type type, String zone, String nodeName) {
    return new Node("", type, null, 0, Collections.emptyMap());
  }
}
