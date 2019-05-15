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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
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
import com.google.api.services.compute.model.Network;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.compute.model.NetworkList;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
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
import com.google.common.io.CharStreams;
import io.cdap.cdap.runtime.spi.provisioner.Node;
import io.cdap.cdap.runtime.spi.provisioner.RetryableProvisionException;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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

/**
 * Wrapper around the dataproc client that adheres to our configuration settings.
 */
public class DataprocClient implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(DataprocClient.class);
  // something like 2018-04-16T12:09:03.943-07:00
  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSX");

  private final DataprocConf conf;
  private final ClusterControllerClient client;
  private final Compute compute;
  private final String projectId;
  private final String network;
  private final String zone;
  private final boolean useInternalIP;

  public static DataprocClient fromConf(DataprocConf conf) throws IOException, GeneralSecurityException {
    // if project, network, or zone are null, try to auto-detect them

    // If CDAP is running on a GCE or GKE VM, the project of the VM can be fetched from the VM metadata
    String project = conf.getProjectId();
    String systemProject = null;
    if (project == null) {
      systemProject = getSystemProjectId();
      project = systemProject;
      LOG.debug("Auto-detected project {}", project);
    }

    // There are two ways to determine the network
    //   1. CDAP is running on a GCE or GKE VM and the configured project is the same as the VM project
    //      In this scenario, we can use the same network that CDAP is in, which will allow the provisioner
    //      to use internal IPs to communicate with the cluster, removing the need to check firewalls, etc.
    //   2. List networks in the configured project with the Compute API and pick one.
    //      The might end up picking a network that is not actually desired, in which case the user should
    //      explicitly specify the network and not leave it as auto-detect.
    String network = conf.getNetwork();
    String systemNetwork = null;
    try {
      systemNetwork = getSystemNetwork();
    } catch (IllegalArgumentException e) {
      // expected when not running on GCP
    }
    Compute compute = getCompute(conf);
    if (network == null && project.equals(systemProject)) {
      network = systemNetwork;
    } else {
      NetworkList networkList = compute.networks().list(project).execute();
      List<Network> networks = networkList.getItems();
      if (networks != null) {
        for (Network projectNetwork : networks) {
          if (projectNetwork != null && projectNetwork.getName() != null) {
            network = projectNetwork.getName();
            LOG.debug("Auto-detected network {} from project {}", network, project);
            break;
          }
        }
      }
    }
    if (network == null) {
      throw new IllegalArgumentException("Could not detect a network to use. Please explicitly set the network.");
    }

    // If CDAP is running on a GCE or GKE VM, use the same zone as the VM
    // otherwise, the user needs to specify the zone
    String zone = conf.getZone();
    if (zone == null) {
      zone = getSystemZone();
      LOG.debug("Auto-detected zone {}", zone);
    }

    // fill in the null values with the detected values
    conf = new DataprocConf(conf, zone, project, network);
    ClusterControllerClient client = getClusterControllerClient(conf);

    boolean useInternalIP = network.equals(systemNetwork) && project.equals(systemProject) &&
      !conf.isPreferExternalIP();
    return new DataprocClient(conf, client, compute, useInternalIP);
  }

  private static ClusterControllerClient getClusterControllerClient(DataprocConf conf) throws IOException {
    CredentialsProvider credentialsProvider =
      FixedCredentialsProvider.create(getDataprocCredentials(conf.getAccountKey()));

    ClusterControllerSettings controllerSettings = ClusterControllerSettings.newBuilder()
      .setCredentialsProvider(credentialsProvider)
      .build();
    return ClusterControllerClient.create(controllerSettings);
  }

  private static Compute getCompute(DataprocConf conf) throws GeneralSecurityException, IOException {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    return new Compute.Builder(httpTransport, JacksonFactory.getDefaultInstance(),
                               getComputeCredential(conf.getAccountKey()))
      .setApplicationName("cdap")
      .build();
  }

  private DataprocClient(DataprocConf conf, ClusterControllerClient client, Compute compute,
                         boolean useInternalIP) {
    this.projectId = conf.getProjectId();
    this.network = conf.getNetwork();
    this.zone = conf.getZone();
    this.useInternalIP = useInternalIP;
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
      // override any os-login that may be set on the project-level metadata
      metadata.put("enable-oslogin", "false");

      GceClusterConfig.Builder clusterConfig = GceClusterConfig.newBuilder()
        .setNetworkUri(conf.getNetwork())
        .addServiceAccountScopes("https://www.googleapis.com/auth/cloud-platform")
        .setZoneUri(conf.getZone())
        .putAllMetadata(metadata);
      if (!useInternalIP) {
        for (String targetTag : getFirewallTargetTags()) {
          clusterConfig.addTags(targetTag);
        }
      }

      Map<String, String> dataprocProps = new HashMap<>(conf.getDataprocProperties());
      // The additional property is needed to be able to provision a singlenode cluster on
      // dataproc. Dataproc has an issue that it will treat 0 number of worker
      // nodes as the default number, which means it will always provision a
      // cluster with 2 worker nodes if this property is not set. Refer to
      // https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/single-node-clusters
      // for more information.
      dataprocProps.put("dataproc:dataproc.allow.zero.workers", "true");
      // Enable/Disable stackdriver
      dataprocProps.put("dataproc:dataproc.logging.stackdriver.enable",
                        Boolean.toString(conf.isStackdriverLoggingEnabled()));
      dataprocProps.put("dataproc:dataproc.monitoring.stackdriver.enable",
                        Boolean.toString(conf.isStackdriverMonitoringEnabled()));


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
                     .setSoftwareConfig(SoftwareConfig.newBuilder()
                                          .setImageVersion(imageVersion)
                                          .putAllProperties(dataprocProps))
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
  public io.cdap.cdap.runtime.spi.provisioner.ClusterStatus getClusterStatus(String name)
    throws RetryableProvisionException {
    return getDataprocCluster(name).map(cluster -> convertStatus(cluster.getStatus()))
      .orElse(io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.NOT_EXISTS);
  }

  /**
   * Get information about the specified cluster. The cluster will not be present if it could not be found.
   *
   * @param name the cluster name
   * @return the cluster information if it exists
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  public Optional<io.cdap.cdap.runtime.spi.provisioner.Cluster> getCluster(String name)
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
    return Optional.of(new io.cdap.cdap.runtime.spi.provisioner.Cluster(
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

    // Iterate all firewall rules and see if it has ingress rules for all required firewall port.
    for (Firewall firewall : Optional.ofNullable(firewalls.getItems()).orElse(Collections.emptyList())) {
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
    if (useInternalIP) {
      ip = properties.get("ip.internal");
    }
    return new Node(nodeName, type, ip, ts, properties);
  }

  private io.cdap.cdap.runtime.spi.provisioner.ClusterStatus convertStatus(ClusterStatus status) {
    switch (status.getState()) {
      case ERROR:
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.FAILED;
      case RUNNING:
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.RUNNING;
      case CREATING:
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.CREATING;
      case DELETING:
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.DELETING;
      case UPDATING:
        // not sure if this is correct, or how it can get to updating state
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.RUNNING;
      default:
        // unrecognized and unknown
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.ORPHANED;
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
   * @return GoogleCredential for use with Compute
   * @throws IOException if there was an error reading the account key
   */
  private static GoogleCredential getComputeCredential(String accountKey) throws IOException {
    if (accountKey == null) {
      return GoogleCredential.getApplicationDefault();
    }

    try (InputStream is = new ByteArrayInputStream(accountKey.getBytes(StandardCharsets.UTF_8))) {
      return GoogleCredential.fromStream(is)
        .createScoped(Collections.singleton("https://www.googleapis.com/auth/cloud-platform"));
    }
  }

  /**
   * @return GoogleCredentials for use with Dataproc
   * @throws IOException if there was an error reading the account key
   */
  private static GoogleCredentials getDataprocCredentials(String accountKey) throws IOException {
    if (accountKey == null) {
      return getComputeEngineCredentials();
    }

    try (InputStream is = new ByteArrayInputStream(accountKey.getBytes(StandardCharsets.UTF_8))) {
      return GoogleCredentials.fromStream(is);
    }
  }

  private static GoogleCredentials getComputeEngineCredentials() throws IOException {
    try {
      GoogleCredentials credentials = ComputeEngineCredentials.create();
      credentials.refreshAccessToken();
      return credentials;
    } catch (IOException e) {
      throw new IOException("Unable to get credentials from the environment. "
                              + "Please explicitly set the account key.", e);
    }
  }

  /**
   * Get network from the metadata server.
   */
  private static String getSystemNetwork() {
    try {
      String network = getMetadata("instance/network-interfaces/0/network");
      // will be something like projects/<project-number>/networks/default
      return network.substring(network.lastIndexOf('/') + 1);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to get the network from the environment. "
                                           + "Please explicitly set the network.", e);
    }
  }

  /**
   * Get zone from the metadata server.
   */
  private static String getSystemZone() {
    try {
      String zone = getMetadata("instance/zone");
      // will be something like projects/<project-number>/zones/us-east1-b
      return zone.substring(zone.lastIndexOf('/') + 1);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to get the zone from the environment. "
                                           + "Please explicitly set the zone.", e);
    }
  }

  /**
   * Get project id from the metadata server.
   */
  private static String getSystemProjectId() {
    try {
      return getMetadata("project/project-id");
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to get project id from the environment. "
                                           + "Please explicitly set the project id and account key.", e);
    }
  }

  /**
   * Makes a request to the metadata server that lives on the VM, as described at
   * https://cloud.google.com/compute/docs/storing-retrieving-metadata.
   */
  private static String getMetadata(String resource) throws IOException {
    URL url = new URL("http://metadata.google.internal/computeMetadata/v1/" + resource);
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestProperty("Metadata-Flavor", "Google");
      connection.connect();
      try (Reader reader = new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8)) {
        return CharStreams.toString(reader);
      }
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
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
