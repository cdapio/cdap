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

import co.cask.cdap.runtime.spi.provisioner.Node;
import co.cask.cdap.runtime.spi.provisioner.RetryableProvisionException;
import co.cask.cdap.runtime.spi.ssh.SSHPublicKey;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Throwables;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.longrunning.OperationSnapshot;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.AccessConfig;
import com.google.api.services.compute.model.Firewall;
import com.google.api.services.compute.model.FirewallList;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.cloud.dataproc.v1.Cluster;
import com.google.cloud.dataproc.v1.ClusterConfig;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.ClusterControllerSettings;
import com.google.cloud.dataproc.v1.ClusterStatus;
import com.google.cloud.dataproc.v1.DeleteClusterRequest;
import com.google.cloud.dataproc.v1.DiskConfig;
import com.google.cloud.dataproc.v1.GceClusterConfig;
import com.google.cloud.dataproc.v1.GetClusterRequest;
import com.google.cloud.dataproc.v1.InstanceGroupConfig;
import com.google.cloud.dataproc.v1.SoftwareConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Wrapper around the dataproc client that adheres to our configuration settings.
 */
public class DataprocClient implements AutoCloseable {
  // something like 2018-04-16T12:09:03.943-07:00
  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSX");

  private final DataprocConf conf;
  private final ClusterControllerClient client;
  private final Compute compute;
  private final String projectId;
  private final String network;
  private final String zone;
  private final String systemNetwork;

  public static DataprocClient fromConf(DataprocConf conf) throws IOException, GeneralSecurityException {
    String systemNetwork = null;
    try {
      systemNetwork = DataprocConf.getSystemNetwork();
    } catch (IllegalArgumentException e) {
      // expected when not running on GCP
    }

    ClusterControllerClient client = getClusterControllerClient(conf);
    Compute compute = getCompute(conf);

    return new DataprocClient(conf, client, compute, systemNetwork);
  }

  private static ClusterControllerClient getClusterControllerClient(DataprocConf conf) throws IOException {
    CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(conf.getDataprocCredentials());

    ClusterControllerSettings controllerSettings = ClusterControllerSettings.newBuilder()
      .setCredentialsProvider(credentialsProvider)
      .build();
    return ClusterControllerClient.create(controllerSettings);
  }

  private static Compute getCompute(DataprocConf conf) throws GeneralSecurityException, IOException {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    return new Compute.Builder(httpTransport, JacksonFactory.getDefaultInstance(), conf.getComputeCredential())
      .setApplicationName("cdap")
      .build();
  }

  private DataprocClient(DataprocConf conf, ClusterControllerClient client, Compute compute,
                         @Nullable String systemNetwork) {
    this.projectId = conf.getProjectId();
    this.network = conf.getNetwork();
    this.zone = conf.getZone();
    this.systemNetwork = systemNetwork;
    this.conf = conf;
    this.client = client;
    this.compute = compute;
  }

  /**
   * Create a cluster. This will return after the initial request to create the cluster is completed.
   * At this point, the cluster is likely not yet running, but in a provisioning state.
   *
   * @param name the name of the cluster to create
   * @param imageVersion the image version for the cluster
   * @param labels labels to set on the cluster
   * @return the response for issuing the create
   * @throws InterruptedException if the thread was interrupted while waiting for the initial request to complete
   * @throws AlreadyExistsException if the cluster already exists
   * @throws IOException if there was an I/O error talking to Google Compute APIs
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  public OperationSnapshot createCluster(String name, String imageVersion, Map<String, String> labels)
    throws RetryableProvisionException, InterruptedException, IOException {

    try {
      Map<String, String> metadata = new HashMap<>();
      SSHPublicKey publicKey = conf.getPublicKey();
      if (publicKey != null) {
        // Don't fail if there is no public key. It is for tooling case that the key might be generated differently.
        metadata.put("ssh-keys", publicKey.getUser() + ":" + publicKey.getKey());
      }

      GceClusterConfig.Builder clusterConfig = GceClusterConfig.newBuilder()
        .setNetworkUri(conf.getNetwork())
        .addServiceAccountScopes("https://www.googleapis.com/auth/cloud-platform")
        .setZoneUri(conf.getZone())
        .putAllMetadata(metadata);
      for (String targetTag : getFirewallTargetTags()) {
        clusterConfig.addTags(targetTag);
      }

      Cluster cluster = com.google.cloud.dataproc.v1.Cluster.newBuilder()
        .setClusterName(name)
        .putAllLabels(labels)
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
                     .setGceClusterConfig(clusterConfig.build())
                     // The additional property is needed to be able to provision a singlenode cluster on
                     // dataproc. Dataproc has an issue that it will treat 0 number of worker
                     // nodes as the default number, which means it will always provision a
                     // cluster with 2 worker nodes if this property is not set. Refer to
                     // https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/single-node-clusters
                     // for more information.
                     .setSoftwareConfig(SoftwareConfig.newBuilder().setImageVersion(imageVersion)
                                          .putProperties("dataproc:dataproc.allow.zero.workers", "true"))
                     .build())
        .build();

      return client.createClusterAsync(projectId, conf.getRegion(), cluster).getInitialFuture().get();
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
        .setProjectId(projectId)
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
   * Get the status of the specified cluster.
   *
   * @param name the cluster name
   * @return the cluster status
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  public co.cask.cdap.runtime.spi.provisioner.ClusterStatus getClusterStatus(String name)
    throws RetryableProvisionException {
    return getDataprocCluster(name).map(cluster -> convertStatus(cluster.getStatus()))
      .orElse(co.cask.cdap.runtime.spi.provisioner.ClusterStatus.NOT_EXISTS);
  }

  /**
   * Get information about the specified cluster. The cluster will not be present if it could not be found.
   *
   * @param name the cluster name
   * @return the cluster information if it exists
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  public Optional<co.cask.cdap.runtime.spi.provisioner.Cluster> getCluster(String name)
    throws RetryableProvisionException, IOException {
    Optional<Cluster> clusterOptional = getDataprocCluster(name);
    if (!clusterOptional.isPresent()) {
      return Optional.empty();
    }

    Cluster cluster = clusterOptional.get();

    List<Node> nodes = new ArrayList<>();
    for (String masterName : cluster.getConfig().getMasterConfig().getInstanceNamesList()) {
      nodes.add(getNode(compute, Node.Type.MASTER, masterName));
    }
    for (String workerName : cluster.getConfig().getWorkerConfig().getInstanceNamesList()) {
      nodes.add(getNode(compute, Node.Type.WORKER, workerName));
    }
    return Optional.of(new co.cask.cdap.runtime.spi.provisioner.Cluster(
      cluster.getClusterName(), convertStatus(cluster.getStatus()), nodes, Collections.emptyMap()));
  }

  private Optional<Cluster> getDataprocCluster(String name) throws RetryableProvisionException {
    try {
      return Optional.of(client.getCluster(GetClusterRequest.newBuilder()
                                             .setClusterName(name)
                                             .setProjectId(projectId)
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

  /**
   * Finds ingress firewall rules for the configured network that matches the required firewall port as
   * defined in {@link FirewallPort}.
   *
   * @return a {@link Collection} of tags that need to be added to the VM to have those firewall rules applies
   * @throws IOException If failed to discover those firewall rules
   */
  private Collection<String> getFirewallTargetTags() throws IOException {
    FirewallList firewalls = compute.firewalls().list(projectId).execute();
    List<String> tags = new ArrayList<>();
    Set<FirewallPort> requiredPorts = EnumSet.allOf(FirewallPort.class);
    for (Firewall firewall : firewalls.getItems()) {
      // network is a url like https://www.googleapis.com/compute/v1/projects/<project>/<region>/networks/<name>
      // we want to get the last section of the path and compare to the configured network name
      int idx = firewall.getNetwork().lastIndexOf('/');
      String networkName = idx >= 0 ? firewall.getNetwork().substring(idx + 1) : firewall.getNetwork();
      if (!networkName.equals(network)) {
        continue;
      }

      String direction = firewall.getDirection();
      if (!"INGRESS".equals(direction) || firewall.getAllowed() == null) {
        continue;
      }

      for (Firewall.Allowed allowed : firewall.getAllowed()) {
        String protocol = allowed.getIPProtocol();
        boolean addTag = false;
        if ("ALL".equals(protocol)) {
          requiredPorts.clear();
          addTag = true;
        } else if ("tcp".equals(protocol)) {
          if (allowed.getPorts() == null || allowed.getPorts().contains(String.valueOf(FirewallPort.SSH.port))) {
            requiredPorts.remove(FirewallPort.SSH);
            addTag = true;
          }
        }
        if (addTag && firewall.getTargetTags() != null && !firewall.getTargetTags().isEmpty()) {
          tags.add(firewall.getTargetTags().iterator().next());
        }
      }
    }

    if (!requiredPorts.isEmpty()) {
      String portList = requiredPorts.stream().map(p -> String.valueOf(p.port)).collect(Collectors.joining(","));
      throw new IllegalArgumentException(String.format(
        "Could not find an ingress firewall rule for network '%s' for ports '%s'. " +
          "Please create a rule to allow incoming traffic on those ports for your IP range.",
        network, portList));
    }
    return tags;
  }

  private Node getNode(Compute compute, Node.Type type, String nodeName) throws IOException {
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
      if (network.equals(networkName)) {
        for (AccessConfig accessConfig : networkInterface.getAccessConfigs()) {
          if (accessConfig.getNatIP() != null) {
            properties.put("ip.external", accessConfig.getNatIP());
            break;
          }
        }
        properties.put("ip.internal", networkInterface.getNetworkIP());
      }
    }
    long ts;
    try {
      ts = DATE_FORMAT.parse(instance.getCreationTimestamp()).getTime();
    } catch (ParseException e) {
      ts = -1L;
    }
    String ip = properties.get("ip.external");
    if (network.equals(systemNetwork) && !conf.preferExternalIP()) {
      ip = properties.get("ip.internal");
    }
    return new Node(nodeName, type, ip, ts, properties);
  }

  private co.cask.cdap.runtime.spi.provisioner.ClusterStatus convertStatus(ClusterStatus status) {
    switch (status.getState()) {
      case ERROR:
        return co.cask.cdap.runtime.spi.provisioner.ClusterStatus.FAILED;
      case RUNNING:
        return co.cask.cdap.runtime.spi.provisioner.ClusterStatus.RUNNING;
      case CREATING:
        return co.cask.cdap.runtime.spi.provisioner.ClusterStatus.CREATING;
      case DELETING:
        return co.cask.cdap.runtime.spi.provisioner.ClusterStatus.DELETING;
      case UPDATING:
        // not sure if this is correct, or how it can get to updating state
        return co.cask.cdap.runtime.spi.provisioner.ClusterStatus.RUNNING;
      default:
        // unrecognized and unknown
        return co.cask.cdap.runtime.spi.provisioner.ClusterStatus.ORPHANED;
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

  /**
   * Firewall ports that we're concerned about.
   */
  private enum FirewallPort {
    SSH(22);

    private final int port;

    FirewallPort(int port) {
      this.port = port;
    }
  }
}
