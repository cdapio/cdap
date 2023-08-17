/*
 *  Copyright © 2022 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.ClusterControllerSettings;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;

/**
 * Creates DataprocClients.
 */
public class DefaultDataprocClientFactory implements DataprocClientFactory {

  private final ComputeFactory computeFactory;

  public DefaultDataprocClientFactory() {
    this(new GoogleComputeFactory());
  }

  public DefaultDataprocClientFactory(ComputeFactory computeFactory) {
    this.computeFactory = computeFactory;
  }

  @Override
  public DataprocClient create(DataprocConf conf, boolean requireSSH)
      throws IOException, GeneralSecurityException {
    ClusterControllerClient clusterControllerClient = getClusterControllerClient(conf);
    return requireSSH ? new SshDataprocClient(conf, clusterControllerClient, computeFactory) :
        new RuntimeMonitorDataprocClient(conf, clusterControllerClient, computeFactory);
  }

  public static ClusterControllerClient getClusterControllerClient(DataprocConf conf)
      throws IOException {
    CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(
        conf.getDataprocCredentials());

    String rootUrl = Optional.ofNullable(conf.getRootUrl())
        .orElse(ClusterControllerSettings.getDefaultEndpoint());
    String regionalEndpoint = conf.getRegion() + "-" + rootUrl;

    ClusterControllerSettings controllerSettings = ClusterControllerSettings.newBuilder()
        .setCredentialsProvider(credentialsProvider)
        .setEndpoint(regionalEndpoint)
        .build();
    return ClusterControllerClient.create(controllerSettings);
  }
}
