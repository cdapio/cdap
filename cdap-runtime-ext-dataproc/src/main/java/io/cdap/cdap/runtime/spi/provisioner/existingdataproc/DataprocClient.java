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

package io.cdap.cdap.runtime.spi.provisioner.existingdataproc;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.util.Throwables;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.AccessConfig;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.cloud.dataproc.v1.Cluster;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.ClusterOperationMetadata;
import com.google.cloud.dataproc.v1.GetClusterRequest;
import com.google.cloud.dataproc.v1.UpdateClusterRequest;
import com.google.protobuf.FieldMask;
import io.cdap.cdap.runtime.spi.common.AbstractDataprocClient;
import io.cdap.cdap.runtime.spi.provisioner.Node;
import io.cdap.cdap.runtime.spi.provisioner.RetryableProvisionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Wrapper around the dataproc client that adheres to our configuration settings.
 */
final class DataprocClient extends AbstractDataprocClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(DataprocClient.class);
  // something like 2018-04-16T12:09:03.943-07:00
  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSX");


  private final ExistingDataprocConf conf;
  private final ClusterControllerClient client;
  private final Compute compute;
  private final String projectId;

  static DataprocClient fromConf(ExistingDataprocConf conf) throws IOException, GeneralSecurityException {
    ClusterControllerClient client = getClusterControllerClient(conf.getDataprocCredentials(), conf.getRegion());
    Compute compute = getCompute(conf.getComputeCredential());
    return new DataprocClient(new ExistingDataprocConf(conf), client, compute);
  }

  private DataprocClient(ExistingDataprocConf conf, ClusterControllerClient client, Compute compute) {
    this.projectId = conf.getProjectId();
    this.conf = conf;
    this.client = client;
    this.compute = compute;
  }

  /**
   * Update the Label on Dataproc cluster to indicate usage of CDF. This will be used for later
   * tracking on usage of Dataproc by CDF.
   *
   * @param labels Key/Value pairs to set on the Dataproc cluster.
   * @param name   the cluster name
   */
  public ClusterOperationMetadata setSystemLabels(Map<String, String> labels, String name) throws Exception {
    try {
      Optional<Cluster> clusterOptional = getDataprocCluster(name);
      if (!clusterOptional.isPresent()) {
        throw new Exception("Error retrieving Dataproc cluster " + name);
      }
      Cluster cluster = clusterOptional.get();
      Map<String, String> existingLabels = cluster.getLabelsMap();
      Cluster.Builder clusterSettings = cluster.toBuilder();

      for (Map.Entry<String, String> entry : labels.entrySet()) {
        if (!existingLabels.containsKey(entry.getKey())) {
          clusterSettings.putLabels(entry.getKey(), entry.getValue());
        }
      }
      clusterSettings.build();
      FieldMask updateMask = FieldMask.newBuilder().addPaths("labels").build();
      OperationFuture<Cluster, ClusterOperationMetadata> operationFuture =
        client.updateClusterAsync(UpdateClusterRequest
                                    .newBuilder()
                                    .setProjectId(conf.getProjectId())
                                    .setRegion(conf.getRegion())
                                    .setClusterName(conf.getClusterName())
                                    .setCluster(clusterSettings)
                                    .setUpdateMask(updateMask)
                                    .build());

      return operationFuture.getMetadata().get();

    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ApiException) {
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
  Optional<io.cdap.cdap.runtime.spi.provisioner.Cluster> getCluster(String name)
    throws Exception {
    Optional<Cluster> clusterOptional = getDataprocCluster(name);
    if (!clusterOptional.isPresent()) {
      return Optional.empty();
    }

    Cluster cluster = clusterOptional.get();
    String zone = getZone(cluster.getConfig().getGceClusterConfig().getZoneUri());

    List<Node> nodes = new ArrayList<>();
    for (String masterName : cluster.getConfig().getMasterConfig().getInstanceNamesList()) {
      nodes.add(getNode(compute, Node.Type.MASTER, zone, masterName));
    }

    for (String workerName : cluster.getConfig().getWorkerConfig().getInstanceNamesList()) {
      nodes.add(getNode(compute, Node.Type.WORKER, zone, workerName));
    }

    return Optional.of(new io.cdap.cdap.runtime.spi.provisioner.Cluster(
      cluster.getClusterName(), convertStatus(cluster.getStatus()), nodes, Collections.emptyMap()));
  }


  private Optional<Cluster> getDataprocCluster(String name) throws RetryableProvisionException, NotFoundException {
    try {
      return Optional.of(client.getCluster(GetClusterRequest.newBuilder()
                                             .setClusterName(name)
                                             .setProjectId(projectId)
                                             .setRegion(conf.getRegion())
                                             .build()));

    } catch (ApiException e) {
      if (e.getStatusCode().getCode().getHttpStatusCode() / 100 != 4) {
        // if there was an API exception that was not a 4xx, we can just try again
        throw new RetryableProvisionException(e);
      }
      // otherwise, it's not a retryable failure
      throw e;
    }
  }


  private Node getNode(Compute compute, Node.Type type, String zone, String nodeName) throws IOException {
    Instance instance;
    try {
      instance = compute.instances().get(projectId, zone, nodeName).execute();
    } catch (GoogleJsonResponseException e) {
      // this can happen right after a cluster is created
      if (e.getStatusCode() == 404) {
        return new Node(nodeName, Node.Type.UNKNOWN, "", -1L, Collections.emptyMap());
      }
      throw e;
    }
    Map<String, String> properties = new HashMap<>();
    for (NetworkInterface networkInterface : instance.getNetworkInterfaces()) {
      Path path = Paths.get(networkInterface.getNetwork());
      String networkName = path.getFileName().toString();
      //if (network.equals(networkName)) {
      // if the cluster does not have an external ip then then access config is null
      if (networkInterface.getAccessConfigs() != null) {
        for (AccessConfig accessConfig : networkInterface.getAccessConfigs()) {
          if (accessConfig.getNatIP() != null) {
            properties.put("ip.external", accessConfig.getNatIP());
            break;
          }
        }
        //}
        properties.put("ip.internal", networkInterface.getNetworkIP());
      }
    }
    long ts;
    try {
      ts = DATE_FORMAT.parse(instance.getCreationTimestamp()).getTime();
    } catch (ParseException e) {
      ts = -1L;
    }

    String ip;
    if (properties.containsKey("ip.external")) {
      ip = properties.get("ip.external");
    } else {
      ip = properties.get("ip.internal");
    }

    return new Node(nodeName, type, ip, ts, properties);
  }


  @Override
  public void close() {
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
