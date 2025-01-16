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
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.ClusterControllerSettings;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.provisioner.RetryableProvisionException;
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
  public DataprocClient create(DataprocConf conf, boolean requireSSH, ErrorCategory errorCategory)
      throws IOException, GeneralSecurityException, RetryableProvisionException {
    ClusterControllerClient clusterControllerClient;
    try {
      clusterControllerClient = getClusterControllerClient(conf);
    } catch (Exception e) {
      String errorReason = "Unable to create dataproc cluster controller client.";
      if (e instanceof ApiException) {
        throw DataprocUtils.handleApiException(null, (ApiException) e, errorReason, errorCategory);
      }
      throw new DataprocRuntimeException.Builder()
          .withCause(e)
          .withErrorCategory(errorCategory)
          .withErrorReason(errorReason)
          .withErrorMessage(String.format("%s %s: %s", errorReason, e.getClass().getName(),
              e.getMessage()))
          .build();
    }
    return requireSSH ? new SshDataprocClient(conf, clusterControllerClient, computeFactory,
        errorCategory) : new RuntimeMonitorDataprocClient(conf, clusterControllerClient,
        computeFactory, errorCategory);
  }

  private static ClusterControllerClient getClusterControllerClient(DataprocConf conf)
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
