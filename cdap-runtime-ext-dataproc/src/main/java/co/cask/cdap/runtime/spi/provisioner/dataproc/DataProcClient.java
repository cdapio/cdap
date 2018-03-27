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

  public OperationSnapshot createCluster(String name) throws RetryableProvisionException {

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

      return client.createClusterAsync(conf.getProjectId(), conf.getRegion(), cluster)
        .getInitialFuture().get(10, TimeUnit.SECONDS);
    } catch (ApiException e) {
      throw handleApiException(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ApiException) {
        throw handleApiException((ApiException) cause);
      }
      throw Throwables.propagate(e);
    }  catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RetryableProvisionException(e);
    } catch (TimeoutException e) {
      throw new RetryableProvisionException(e);
    }
  }

  // if there was an API exception that was not a 4xx, we can just try again
  private RetryableProvisionException handleApiException(ApiException e) throws RetryableProvisionException {
    if (e.getStatusCode().getCode().getHttpStatusCode() / 100 != 4) {
      throw new RetryableProvisionException(e);
    }
    throw e;
  }

  @Nullable
  public OperationSnapshot deleteCluster(String name) throws RetryableProvisionException {

    try {
      DeleteClusterRequest request = DeleteClusterRequest.newBuilder()
        .setClusterName(name)
        .setProjectId(conf.getProjectId())
        .setRegion(conf.getRegion())
        .build();

      return client.deleteClusterAsync(request).getInitialFuture().get(10, TimeUnit.SECONDS);
    } catch (ApiException e) {
      if (e.getStatusCode().getCode().getHttpStatusCode() == 404) {
        // if the cluster was not found, it's ok that means it's deleted
        return null;
      }
      throw handleApiException(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ApiException) {
        throw handleApiException((ApiException) cause);
      }
      throw Throwables.propagate(e);
    }  catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RetryableProvisionException(e);
    } catch (TimeoutException e) {
      throw new RetryableProvisionException(e);
    }
  }

  @Nullable
  public Cluster getCluster(String name) throws RetryableProvisionException {
    try {
      return client.getCluster(GetClusterRequest.newBuilder()
                                 .setClusterName(name)
                                 .setProjectId(conf.getProjectId())
                                 .setRegion(conf.getRegion())
                                 .build());
    } catch (NotFoundException e) {
      return null;
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
}
