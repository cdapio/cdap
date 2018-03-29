/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.runtime.spi.provisioner.dataproc;

import co.cask.cdap.runtime.spi.provisioner.RetryableProvisionException;
import com.google.api.client.util.Throwables;
import com.google.api.gax.longrunning.OperationSnapshot;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.dataproc.v1.Cluster;
import com.google.cloud.dataproc.v1.ClusterConfig;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.DeleteClusterRequest;
import com.google.cloud.dataproc.v1.DiskConfig;
import com.google.cloud.dataproc.v1.GceClusterConfig;
import com.google.cloud.dataproc.v1.GetClusterRequest;
import com.google.cloud.dataproc.v1.InstanceGroupConfig;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Wrapper around the dataproc client that adheres to our configuration settings.
 */
public class DataProcClient implements AutoCloseable {
  private final DataProcConf conf;
  private final ClusterControllerClient client;

  public static DataProcClient fromConf(DataProcConf conf) throws IOException {
    ClusterControllerClient client = ClusterControllerClient.create(conf.getControllerSettings());
    return new DataProcClient(conf, client);
  }

  private DataProcClient(DataProcConf conf, ClusterControllerClient client) {
    this.conf = conf;
    this.client = client;
  }

  /**
   * Create a cluster. This will return after the initial request to create the cluster is completed.
   * At this point, the cluster is likely not yet running, but in a provisioning state.
   *
   * @param name the name of the cluster to create
   * @return the response for issuing the create
   * @throws InterruptedException if the thread was interrupted while waiting for the initial request to complete
   * @throws AlreadyExistsException if the cluster already exists
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  public OperationSnapshot createCluster(String name) throws RetryableProvisionException, InterruptedException {

    // TODO: figure out how to set labels
    try {
      Cluster cluster = com.google.cloud.dataproc.v1.Cluster.newBuilder()
        .setClusterName(name)
        .setConfig(ClusterConfig.newBuilder()
                     .setMasterConfig(InstanceGroupConfig.newBuilder()
                                        .setNumInstances(conf.getMasterNumNodes())
                                        .setMachineTypeUri(conf.getMasterMachineType())
                                        .setDiskConfig(DiskConfig.newBuilder()
                                                         .setBootDiskSizeGb(conf.getMasterDiskGB())
                                                         .setNumLocalSsds(0)
                                                         .build())
                                        .build())
                     .setWorkerConfig(InstanceGroupConfig.newBuilder()
                                        .setNumInstances(conf.getWorkerNumNodes())
                                        .setMachineTypeUri(conf.getWorkerMachineType())
                                        .setDiskConfig(DiskConfig.newBuilder()
                                                         .setBootDiskSizeGb(conf.getWorkerDiskGB())
                                                         .setNumLocalSsds(0)
                                                         .build())
                                        .build())
                     .setGceClusterConfig(GceClusterConfig.newBuilder()
                                            .setSubnetworkUri("default")
                                            .setZoneUri(conf.getZone())
                                            .build())
                     .build())
        .build();

      return client.createClusterAsync(conf.getProjectId(), conf.getRegion(), cluster).getInitialFuture().get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ApiException) {
        throw handleApiException((ApiException) cause);
      }
      throw Throwables.propagate(e);
    }
  }

  /**
   * Delete the specified cluster if it exists. This will return after the initial request to delete the cluster
   * is completed. At this point, the cluster is likely not yet deleted, but in a deleting state.
   *
   * @param name the name of the cluster to delete
   * @return the response for issuing the delete, or empty if the cluster already does not exist
   * @throws InterruptedException if the thread was interrupted while waiting for the initial request to complete
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  public Optional<OperationSnapshot> deleteCluster(String name)
    throws RetryableProvisionException, InterruptedException {

    try {
      DeleteClusterRequest request = DeleteClusterRequest.newBuilder()
        .setClusterName(name)
        .setProjectId(conf.getProjectId())
        .setRegion(conf.getRegion())
        .build();

      return Optional.of(client.deleteClusterAsync(request).getInitialFuture().get());
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ApiException) {
        ApiException apiException = (ApiException) cause;
        if (apiException.getStatusCode().getCode().getHttpStatusCode() == 404) {
          // if the cluster was not found, it's ok that means it's deleted
          return Optional.empty();
        }
        throw handleApiException((ApiException) cause);
      }
      throw Throwables.propagate(e);
    }
  }

  /**
   * Get information about the specified cluster. The cluster will not be present if it could not be found.
   *
   * @param name the cluster name
   * @return the cluster information if it exists
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  public Optional<Cluster> getCluster(String name) throws RetryableProvisionException {
    try {
      return Optional.of(client.getCluster(GetClusterRequest.newBuilder()
                                             .setClusterName(name)
                                             .setProjectId(conf.getProjectId())
                                             .setRegion(conf.getRegion())
                                             .build()));
    } catch (NotFoundException e) {
      return Optional.empty();
    } catch (ApiException e) {
      if (e.getStatusCode().getCode().getHttpStatusCode() / 100 != 4) {
        // if there was an API exception that was not a 4xx, we can just try again
        throw new RetryableProvisionException(e);
      }
      // otherwise, it's not a retryable failure
      throw e;
    }
  }

  @Override
  public void close() throws Exception {
    client.close();
  }

  // if there was an API exception that was not a 4xx, we can just try again
  private RetryableProvisionException handleApiException(ApiException e) throws RetryableProvisionException {
    if (e.getStatusCode().getCode().getHttpStatusCode() / 100 != 4) {
      throw new RetryableProvisionException(e);
    }
    throw e;
  }
}
