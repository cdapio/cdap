/*
 * Copyright © 2018-2020 Cask Data, Inc.
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

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.AccessConfig;
import com.google.api.services.compute.model.Firewall;
import com.google.api.services.compute.model.FirewallList;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.Network;
import com.google.api.services.compute.model.NetworkList;
import com.google.api.services.compute.model.NetworkPeering;
import com.google.cloud.dataproc.v1beta2.Cluster;
import com.google.cloud.dataproc.v1beta2.ClusterConfig;
import com.google.cloud.dataproc.v1beta2.ClusterControllerClient;
import com.google.cloud.dataproc.v1beta2.ClusterControllerSettings;
import com.google.cloud.dataproc.v1beta2.ClusterOperationMetadata;
import com.google.cloud.dataproc.v1beta2.ClusterStatus;
import com.google.cloud.dataproc.v1beta2.DeleteClusterRequest;
import com.google.cloud.dataproc.v1beta2.DiskConfig;
import com.google.cloud.dataproc.v1beta2.EncryptionConfig;
import com.google.cloud.dataproc.v1beta2.EndpointConfig;
import com.google.cloud.dataproc.v1beta2.GceClusterConfig;
import com.google.cloud.dataproc.v1beta2.GetClusterRequest;
import com.google.cloud.dataproc.v1beta2.InstanceGroupConfig;
import com.google.cloud.dataproc.v1beta2.NodeInitializationAction;
import com.google.cloud.dataproc.v1beta2.SoftwareConfig;
import com.google.cloud.dataproc.v1beta2.UpdateClusterRequest;
import com.google.longrunning.Operation;
import com.google.longrunning.OperationsClient;
import com.google.protobuf.FieldMask;
import com.google.rpc.Status;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.common.IPRange;
import io.cdap.cdap.runtime.spi.provisioner.Node;
import io.cdap.cdap.runtime.spi.provisioner.RetryableProvisionException;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
final class DataprocClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(DataprocClient.class);
  // something like 2018-04-16T12:09:03.943-07:00
  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSX");
  private static final List<IPRange> PRIVATE_IP_RANGES = DataprocUtils.parseIPRanges(Arrays.asList("10.0.0.0/8",
                                                                                                   "172.16.0.0/12",
                                                                                                   "192.168.0.0/16"));
  static final String DATAPROC_GOOGLEAPIS_COM_443 = "-dataproc.googleapis.com:443";
  private final DataprocConf conf;
  private final ClusterControllerClient client;
  private final Compute compute;
  private final Network network;

  private enum PeeringState {
    ACTIVE,
    INACTIVE,
    NONE
  }

  /**
   * Creates a {@link DataprocClient} from the given {@link DataprocConf}.
   *
   * @param conf the configuration for the client
   * @return a {@link DataprocClient} instance for using Dataproc API
   * @throws IOException if failed to connect to GCP api during the client creation
   * @throws GeneralSecurityException if the client is failed to authenticate
   */
  static DataprocClient fromConf(DataprocConf conf) throws IOException, GeneralSecurityException {
    ClusterControllerClient client = getClusterControllerClient(conf);
    Compute compute = getCompute(conf);

    String network = conf.getNetwork();
    String systemNetwork = null;
    try {
      systemNetwork = DataprocUtils.getSystemNetwork();
    } catch (IllegalArgumentException e) {
      // expected when not running on GCP, ignore
    }

    String projectId = conf.getProjectId();
    String networkHostProjectId = conf.getNetworkHostProjectID();
    String systemProjectId = null;
    try {
      systemProjectId = DataprocUtils.getSystemProjectId();
    } catch (IllegalArgumentException e) {
      // expected when not running on GCP, ignore
    }
    if (network == null && projectId.equals(systemProjectId)) {
      // If the CDAP instance is running on a GCE/GKE VM from a project that matches the provisioner project,
      // use the network of that VM.
      network = systemNetwork;
    } else if (network == null) {
      // Otherwise, pick a network from the configured project using the Compute API

      network = findNetwork(networkHostProjectId, compute);
    }
    if (network == null) {
      throw new IllegalArgumentException("Unable to automatically detect a network, please explicitly set a network.");
    }

    String subnet = conf.getSubnet();
    Network networkInfo = getNetworkInfo(networkHostProjectId, network, compute);

    List<String> subnets = networkInfo.getSubnetworks();
    if (subnet != null && !subnetExists(subnets, subnet)) {
      throw new IllegalArgumentException(String.format("Subnet '%s' does not exist in network '%s' in project '%s'. "
                                                         + "Please use a different subnet.",
                                                       subnet, network, networkHostProjectId));
    }

    // if the network uses custom subnets, a subnet must be provided to the dataproc api
    boolean autoCreateSubnet = networkInfo.getAutoCreateSubnetworks() == null ?
      false : networkInfo.getAutoCreateSubnetworks();
    if (!autoCreateSubnet) {
      // if the network uses custom subnets but none exist, error out
      if (subnets == null || subnets.isEmpty()) {
        throw new IllegalArgumentException(String.format("Network '%s' in project '%s' does not contain any subnets. "
                                                           + "Please create a subnet or use a different network.",
                                                         network, networkHostProjectId));
      }
    }

    subnet = chooseSubnet(network, subnets, subnet, conf.getRegion());

    return new DataprocClient(new DataprocConf(conf, network, subnet), client, compute, networkInfo);
  }

  private static PeeringState getPeeringState(String systemProjectId, String systemNetwork, Network networkInfo) {
    // note: vpc network is a global resource.
    // https://cloud.google.com/compute/docs/regions-zones/global-regional-zonal-resources#globalresources
    String systemNetworkPath = String.format("https://www.googleapis.com/compute/v1/projects/%s/global/networks/%s",
                                             systemProjectId, systemNetwork);

    LOG.trace(String.format("Self link for the system network is %s", systemNetworkPath));
    List<NetworkPeering> peerings = networkInfo.getPeerings();
    // if the customer does not has a peering established at all the peering list is null
    if (peerings == null) {
      return PeeringState.NONE;
    }
    for (NetworkPeering peering : peerings) {
      if (!systemNetworkPath.equals(peering.getNetwork())) {
        continue;
      }
      return peering.getState().equals("ACTIVE") ? PeeringState.ACTIVE : PeeringState.INACTIVE;
    }
    return PeeringState.NONE;
  }

  private static boolean subnetExists(List<String> subnets, String subnet) {
    // subnets are of the form
    // "https://www.googleapis.com/compute/v1/projects/<project>/regions/<region>/subnetworks/<name>"
    // the provided subnet can be the full URI but is most often just the name
    for (String networkSubnet : subnets) {
      if (networkSubnet.equals(subnet) || networkSubnet.endsWith("subnetworks/" + subnet)) {
        return true;
      }
    }
    return false;
  }

  // subnets are identified as
  // "https://www.googleapis.com/compute/v1/projects/<project>/regions/<region>/subnetworks/<name>"
  // a subnet in the same region as the dataproc cluster must be chosen. If a subnet name is provided then the subnet
  // will be choosen and the region will be picked on basis of the given zone. If a subnet name is not provided then
  // any subnetwork in the region of the given zone will be picked.
  private static String chooseSubnet(String network, List<String> subnets, @Nullable String subnet, String region) {
    for (String currentSubnet : subnets) {
      // if a subnet name is given then get the region of that subnet based on the zone
      if (subnet != null && !currentSubnet.endsWith("subnetworks/" + subnet)) {
        continue;
      }
      if (currentSubnet.contains(region + "/subnetworks")) {
        return currentSubnet;
      }
    }
    throw new IllegalArgumentException(
      String.format("Could not find %s in network '%s' that are for region '%s'", subnet == null ? "any subnet" :
        String.format("a subnet named '%s", subnet), network, region));
  }

  private static String findNetwork(String project, Compute compute) throws IOException {
    NetworkList networkList = compute.networks().list(project).execute();
    List<Network> networks = networkList.getItems();
    if (networks == null || networks.isEmpty()) {
      throw new IllegalArgumentException(String.format("Unable to find any networks in project '%s'. "
                                                         + "Please create a network in the project.", project));
    }

    for (Network network : networks) {
      if ("default".equals(network.getName())) {
        return network.getName();
      }
    }

    return networks.iterator().next().getName();
  }

  private static Network getNetworkInfo(String project, String network, Compute compute) throws IOException {
    Network networkObj = compute.networks().get(project, network).execute();
    if (networkObj == null) {
      throw new IllegalArgumentException(String.format("Unable to find network '%s' in project '%s'. "
                                                         + "Please specify another network.", network, project));
    }
    return networkObj;
  }

  /**
   * Extracts and returns the zone name from the given full zone URI.
   */
  private static String getZone(String zoneUri) {
    int idx = zoneUri.lastIndexOf("/");
    if (idx <= 0) {
      throw new IllegalArgumentException("Invalid zone uri " + zoneUri);
    }
    return zoneUri.substring(idx + 1);
  }

  /*
   * Using the input Google Credentials retrieve the Dataproc Cluster controller client
   */
  private static ClusterControllerClient getClusterControllerClient(DataprocConf conf) throws IOException {
    CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(conf.getDataprocCredentials());

    String regionalEndpoint = conf.getRegion() + DATAPROC_GOOGLEAPIS_COM_443;

    ClusterControllerSettings controllerSettings = ClusterControllerSettings.newBuilder()
      .setCredentialsProvider(credentialsProvider)
      .setEndpoint(regionalEndpoint)
      .build();
    return ClusterControllerClient.create(controllerSettings);
  }

  /*
   * Retrieve Google Compute Instance using Credentials
   */
  private static Compute getCompute(DataprocConf conf) throws GeneralSecurityException, IOException {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    return new Compute.Builder(httpTransport, JacksonFactory.getDefaultInstance(), conf.getComputeCredential())
      .setApplicationName("cdap")
      .build();
  }

  private DataprocClient(DataprocConf conf, ClusterControllerClient client, Compute compute, Network network) {
    this.conf = conf;
    this.client = client;
    this.compute = compute;
    this.network = network;
  }

  /**
   * Create a cluster. This will return after the initial request to create the cluster is completed.
   * At this point, the cluster is likely not yet running, but in a provisioning state.
   *
   * @param name         the name of the cluster to create
   * @param imageVersion the image version for the cluster
   * @param labels       labels to set on the cluster
   * @param privateInstance {@code true} to indicate using private instance
   * @return create operation metadata
   * @throws InterruptedException        if the thread was interrupted while waiting for the initial request to complete
   * @throws AlreadyExistsException      if the cluster already exists
   * @throws IOException                 if there was an I/O error talking to Google Compute APIs
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  ClusterOperationMetadata createCluster(String name, String imageVersion, Map<String, String> labels,
                                         boolean privateInstance)
    throws RetryableProvisionException, InterruptedException, IOException {

    try {
      Map<String, String> metadata = new HashMap<>();
      SSHPublicKey publicKey = conf.getPublicKey();
      if (publicKey != null) {
        // Don't fail if there is no public key. It is for tooling case that the key might be generated differently.
        metadata.put("ssh-keys", publicKey.getUser() + ":" + publicKey.getKey());
        // override any os-login that may be set on the project-level metadata
        // this metadata is only needed if ssh is being used to launch the jobs - CDAP-15369
        metadata.put("enable-oslogin", "false");
      }

      //Check if ClusterMetaData is provided and add them.
      metadata.putAll(conf.getClusterMetaData());

      GceClusterConfig.Builder clusterConfig = GceClusterConfig.newBuilder()
        .addServiceAccountScopes(DataprocConf.CLOUD_PLATFORM_SCOPE)
        .putAllMetadata(metadata);

      if (conf.getServiceAccount() != null) {
        clusterConfig.setServiceAccount(conf.getServiceAccount());
      }

      if (conf.getZone() != null) {
        clusterConfig.setZoneUri(conf.getZone());
      }

      // subnets are unique within a location, not within a network, which is why these configs are mutually exclusive.
      if (conf.getSubnet() != null) {
        clusterConfig.setSubnetworkUri(conf.getSubnet());
      } else {
        clusterConfig.setNetworkUri(network.getSelfLink());
      }

      //Add any defined Network Tags
      clusterConfig.addAllTags(conf.getNetworkTags());
      boolean internalIPOnly = isInternalIPOnly(privateInstance, publicKey != null);

      // if public key is not null that means ssh is used to launch / monitor job on dataproc
      if (publicKey != null) {
        int maxTags = Math.max(0, DataprocConf.MAX_NETWORK_TAGS - clusterConfig.getTagsCount());
        List<String> tags = getFirewallTargetTags(internalIPOnly);
        if (tags.size() > maxTags) {
          LOG.warn("No more than 64 tags can be added. Firewall tags ignored: {}", tags.subList(maxTags, tags.size()));
        }
        tags.stream().limit(maxTags).forEach(clusterConfig::addTags);
      }

      // if internal ip is preferred then create dataproc cluster without external ip for better security
      clusterConfig.setInternalIpOnly(internalIPOnly);

      Map<String, String> clusterProperties = new HashMap<>(conf.getClusterProperties());
      // Enable/Disable stackdriver
      clusterProperties.put("dataproc:dataproc.logging.stackdriver.enable",
                            Boolean.toString(conf.isStackdriverLoggingEnabled()));
      clusterProperties.put("dataproc:dataproc.monitoring.stackdriver.enable",
                            Boolean.toString(conf.isStackdriverMonitoringEnabled()));

      InstanceGroupConfig.Builder instanceGroupBuilder = InstanceGroupConfig.newBuilder()
          .setNumInstances(conf.getWorkerNumNodes())
          .setMachineTypeUri(conf.getWorkerMachineType())
          .setDiskConfig(DiskConfig.newBuilder()
              .setBootDiskSizeGb(conf.getWorkerDiskGB())
              .setNumLocalSsds(0)
              .build());

      SoftwareConfig.Builder softwareConfigBuilder = SoftwareConfig.newBuilder()
          .putAllProperties(clusterProperties);
      //Use image version only if custom Image URI is not specified, otherwise may cause image version conflicts
      if (conf.getCustomImageUri() == null || conf.getCustomImageUri().isEmpty()) {
        softwareConfigBuilder.setImageVersion(imageVersion);
      } else {
        //If custom Image URI is specified, use that for cluster creation
        instanceGroupBuilder.setImageUri(conf.getCustomImageUri());
      }
      ClusterConfig.Builder builder = ClusterConfig.newBuilder()
        .setEndpointConfig(EndpointConfig.newBuilder()
                             .setEnableHttpPortAccess(conf.isComponentGatewayEnabled())
                             .build())
        .setMasterConfig(InstanceGroupConfig.newBuilder()
                           .setNumInstances(conf.getMasterNumNodes())
                           .setMachineTypeUri(conf.getMasterMachineType())
                           .setDiskConfig(DiskConfig.newBuilder()
                                            .setBootDiskSizeGb(conf.getMasterDiskGB())
                                            .setNumLocalSsds(0)
                                            .build())
                           .build())
        .setWorkerConfig(instanceGroupBuilder.build())
        .setGceClusterConfig(clusterConfig.build())
        .setSoftwareConfig(softwareConfigBuilder);

      //Add any Node Initialization action scripts
      for (String action : conf.getInitActions()) {
        builder.addInitializationActions(
          NodeInitializationAction.newBuilder()
            .setExecutableFile(action)
            .build());
      }

      if (conf.getEncryptionKeyName() != null) {
        builder.setEncryptionConfig(EncryptionConfig.newBuilder()
                                      .setGcePdKmsKeyName(conf.getEncryptionKeyName()).build());
      }

      if (conf.getGcsBucket() != null) {
        builder.setConfigBucket(conf.getGcsBucket());
      }

      Cluster cluster = com.google.cloud.dataproc.v1beta2.Cluster.newBuilder()
        .setClusterName(name)
        .putAllLabels(labels)
        .setConfig(builder.build())
        .build();

      OperationFuture<Cluster, ClusterOperationMetadata> operationFuture =
        client.createClusterAsync(conf.getProjectId(), conf.getRegion(), cluster);
      return operationFuture.getMetadata().get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ApiException) {
        throw handleApiException((ApiException) cause);
      }
      throw new DataprocRuntimeException(cause);
    }
  }

  /**
   * Updates labels on the given Dataproc cluster.
   *
   * @param clusterName name of the cluster
   * @param labels Key/Value pairs to set on the Dataproc cluster.
   */
  void updateClusterLabels(String clusterName,
                           Map<String, String> labels) throws RetryableProvisionException, InterruptedException {
    if (labels.isEmpty()) {
      return;
    }
    try {
      Cluster cluster = getDataprocCluster(clusterName)
        .filter(c -> c.getStatus().getState() == ClusterStatus.State.RUNNING)
        .orElseThrow(() -> new DataprocRuntimeException("Dataproc cluster " + clusterName +
                                                          " does not exist or not in running state"));

      FieldMask updateMask = FieldMask.newBuilder().addPaths("labels").build();
      OperationFuture<Cluster, ClusterOperationMetadata> operationFuture =
        client.updateClusterAsync(UpdateClusterRequest
                                    .newBuilder()
                                    .setProjectId(conf.getProjectId())
                                    .setRegion(conf.getRegion())
                                    .setClusterName(clusterName)
                                    .setCluster(cluster.toBuilder().putAllLabels(labels))
                                    .setUpdateMask(updateMask)
                                    .build());

      ClusterOperationMetadata metadata = operationFuture.getMetadata().get();
      int numWarnings = metadata.getWarningsCount();
      if (numWarnings > 0) {
        LOG.warn("Encountered {} warning{} while setting labels on cluster:\n{}",
                 numWarnings, numWarnings > 1 ? "s" : "", String.join("\n", metadata.getWarningsList()));
      }
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ApiException) {
        throw handleApiException((ApiException) cause);
      }
      throw new DataprocRuntimeException(cause);
    }
  }

  /**
   * Delete the specified cluster if it exists. This will return after the initial request to delete the cluster
   * is completed. At this point, the cluster is likely not yet deleted, but in a deleting state.
   *
   * @param name the name of the cluster to delete
   * @throws InterruptedException        if the thread was interrupted while waiting for the initial request to complete
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  Optional<ClusterOperationMetadata> deleteCluster(String name)
    throws RetryableProvisionException, InterruptedException {
    try {
      DeleteClusterRequest request = DeleteClusterRequest.newBuilder()
        .setClusterName(name)
        .setProjectId(conf.getProjectId())
        .setRegion(conf.getRegion())
        .build();

      return Optional.of(client.deleteClusterAsync(request).getMetadata().get());
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ApiException) {
        ApiException apiException = (ApiException) cause;
        if (apiException.getStatusCode().getCode().getHttpStatusCode() == 404) {
          // if the cluster was not found, it's ok that means it's deleted
          return Optional.empty();
        }
        // Sometimes the cluster could not be created because firewall rules prevent communication between nodes.
        // In this situation, the cluster create operation fails after some timeout and the cluster transitions
        // to the DELETING state on its own.
        // If a delete call is then made, the delete call fails because the API does not allow deleting a cluster
        // that is in the DELETING state
        // In general, if the cluster is already in the deleting state, behave as if the delete request was successful
        try {
          Optional<Cluster> cluster = getDataprocCluster(name);
          if (!cluster.isPresent() || cluster.get().getStatus().getState() == ClusterStatus.State.DELETING) {
            return Optional.empty();
          }
          // if the cluster isn't gone or in the deleting state, handle the original delete error
        } catch (Exception e1) {
          // if there was an error getting the cluster information, ignore it and handle the original delete error
        }
        throw handleApiException((ApiException) cause);
      }
      throw new DataprocRuntimeException(cause);
    }
  }

  /**
   * Get the status of the specified cluster.
   *
   * @param name the cluster name
   * @return the cluster status
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  io.cdap.cdap.runtime.spi.provisioner.ClusterStatus getClusterStatus(String name)
    throws RetryableProvisionException {
    io.cdap.cdap.runtime.spi.provisioner.ClusterStatus status = getDataprocCluster(name)
      .map(cluster -> convertStatus(cluster.getStatus()))
      .orElse(io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.NOT_EXISTS);

    // if it failed, try to get the create operation and log the error message
    try {
      if (status == io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.FAILED) {
        String resourceName = String.format("projects/%s/regions/%s/operations", conf.getProjectId(), conf.getRegion());
        String filter = String.format("clusterName=%s AND operationType=CREATE", name);
        OperationsClient.ListOperationsPagedResponse operationsResponse =
          client.getOperationsClient().listOperations(resourceName, filter);
        OperationsClient.ListOperationsPage page = operationsResponse.getPage();
        if (page == null) {
          LOG.warn("Unable to get the cause of the cluster creation failure.");
          return status;
        }

        if (page.getPageElementCount() > 1) {
          // shouldn't be possible
          LOG.warn("Multiple create operations found for cluster {}, may not be able to find the failure message.",
                   name);
        }
        if (page.getPageElementCount() > 0) {
          Operation operation = page.getValues().iterator().next();
          Status operationError = operation.getError();
          if (operationError != null) {
            LOG.warn("Failed to create cluster {}: {}", name, operationError.getMessage());
          }
        }
      }
    } catch (Exception e) {
      // if we failed to get the operations list, log an error and proceed with normal execution
      LOG.warn("Unable to get the cause of the cluster creation failure.", e);
    }

    return status;
  }

  /**
   * Get information about the specified cluster. The cluster will not be present if it could not be found.
   *
   * @param name the cluster name
   * @return the cluster information if it exists
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  Optional<io.cdap.cdap.runtime.spi.provisioner.Cluster> getCluster(String name)
    throws RetryableProvisionException, IOException {
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

  private Optional<Cluster> getDataprocCluster(String name) throws RetryableProvisionException {
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

  /**
   * Determines if the Dataproc cluster is private IP only.
   *
   * @param privateInstance a system config to force using private instance
   * @param sshRuntimeMonitor {@code true} if SSH is used for runtime monitoring
   * @return {@code true} for pribvate IP only Dataproc cluster
   */
  private boolean isInternalIPOnly(boolean privateInstance, boolean sshRuntimeMonitor) {
    String systemProjectId = null;
    String systemNetwork = null;
    try {
      systemProjectId = DataprocUtils.getSystemProjectId();
      systemNetwork = DataprocUtils.getSystemNetwork();
    } catch (IllegalArgumentException e) {
      // expected when not running on GCP, ignore
    }

    // Use private IP only cluster if privateInstance is true or if the compute profile required
    if (!privateInstance && conf.isPreferExternalIP()) {
      return false;
    }

    // If it is forced to be private instance or
    // if CDAP runs in GCP project and runtime job manager is used and monitoring is not done through SSH,
    // then we don't need to validate network connectivity
    if (privateInstance || (systemProjectId != null && conf.isRuntimeJobManagerEnabled() && !sshRuntimeMonitor)) {
      return true;
    }

    // If the CDAP is not running on GCP VM, then we just honor the prefer external IP config
    if (systemProjectId == null || systemNetwork == null) {
      return true;
    }

    // SSH will be used for job launching and/or monitoring, we need to validate network connectivity
    // CDAP and Dataproc are in the same network, should be able to use private IP only cluster
    if (systemProjectId.equals(conf.getNetworkHostProjectID()) && systemNetwork.equals(network.getName())) {
      return true;
    }

    // Check network is peering, we can use private ip only cluster
    PeeringState state = getPeeringState(systemProjectId, systemNetwork, network);
    if (state == PeeringState.ACTIVE) {
      return true;
    }

    // If there is no network connectivity and yet private ip only cluster is requested, raise an exception
    throw new DataprocRuntimeException(
      String.format("Direct network connectivity is needed for private Dataproc cluster between VPC %s/%s and %s/%s",
                    systemProjectId, systemNetwork, conf.getNetworkHostProjectID(), network.getName())
    );
  }

  /**
   * Finds ingress firewall rules for the configured network that matches the required firewall port as
   * defined in {@link FirewallPort}.
   *
   * @return a {@link Collection} of tags that need to be added to the VM to have those firewall rules applies
   * @throws IOException If failed to discover those firewall rules
   */
  private List<String> getFirewallTargetTags(boolean useInternalIP) throws IOException {
    FirewallList firewalls = compute.firewalls().list(conf.getNetworkHostProjectID()).execute();
    List<String> tags = new ArrayList<>();
    Set<FirewallPort> requiredPorts = EnumSet.allOf(FirewallPort.class);

    // Iterate all firewall rules and see if it has ingress rules for all required firewall port.
    for (Firewall firewall : Optional.ofNullable(firewalls.getItems()).orElse(Collections.emptyList())) {
      // network is a url like https://www.googleapis.com/compute/v1/projects/<project>/<region>/networks/<name>
      // we want to get the last section of the path and compare to the configured network name
      int idx = firewall.getNetwork().lastIndexOf('/');
      String networkName = idx >= 0 ? firewall.getNetwork().substring(idx + 1) : firewall.getNetwork();
      if (!networkName.equals(network.getName())) {
        continue;
      }

      String direction = firewall.getDirection();
      if (!"INGRESS".equals(direction) || firewall.getAllowed() == null) {
        continue;
      }

      if (useInternalIP) {
        // If the Dataproc cluster is using internal IP only, we are only interested in firewall rule that has source
        // IP range overlap with one of the private IP block or doesn't have source IP at all.
        // This is because if Dataproc cluster is using internal IP, the CDAP itself must be running inside one of the
        // private IP blocks in order to be able to communicate with Dataproc.
        try {
          List<IPRange> sourceRanges = Optional.ofNullable(firewall.getSourceRanges())
            .map(DataprocUtils::parseIPRanges)
            .orElse(Collections.emptyList());

          if (!sourceRanges.isEmpty()) {
            boolean isPrivate = PRIVATE_IP_RANGES.stream()
              .anyMatch(privateRange -> sourceRanges.stream().anyMatch(privateRange::isOverlap));
            if (!isPrivate) {
              continue;
            }
          }
        } catch (Exception e) {
          LOG.warn("Failed to parse source ranges from firewall rule {}", firewall.getName(), e);
        }
      }

      for (Firewall.Allowed allowed : firewall.getAllowed()) {
        String protocol = allowed.getIPProtocol();
        boolean addTag = false;
        if ("all".equalsIgnoreCase(protocol)) {
          requiredPorts.clear();
          addTag = true;
        } else if ("tcp".equalsIgnoreCase(protocol) && isPortAllowed(allowed.getPorts(), FirewallPort.SSH.port)) {
          requiredPorts.remove(FirewallPort.SSH);
          addTag = true;
        }
        if (addTag && firewall.getTargetTags() != null && !firewall.getTargetTags().isEmpty()) {
          tags.add(firewall.getTargetTags().iterator().next());
        }
      }
    }

    if (!requiredPorts.isEmpty()) {
      String portList = requiredPorts.stream().map(p -> String.valueOf(p.port)).collect(Collectors.joining(","));
      throw new IllegalArgumentException(String.format(
        "Could not find an ingress firewall rule for network '%s' in project '%s' for ports '%s'. " +
          "Please create a rule to allow incoming traffic on those ports for your IP range.",
        network.getName(), conf.getNetworkHostProjectID(), portList));
    }
    return tags;
  }

  /**
   * Returns if the given port is allowed by the list of allowed ports. The allowed ports is in format as allowed by
   * GCP firewall rule.
   */
  private boolean isPortAllowed(@Nullable List<String> allowedPorts, int port) {
    if (allowedPorts == null) {
      return true;
    }
    for (String allowedPort : allowedPorts) {
      int idx = allowedPort.lastIndexOf('-');
      try {
        // This is a port range specification in format of "startPort-endPort" (e.g. 0-65535)
        if (idx > 0) {
          int fromPort = Integer.parseInt(allowedPort.substring(0, idx));
          int toPort = Integer.parseInt(allowedPort.substring(idx + 1));
          if (port >= fromPort && port <= toPort) {
            return true;
          }
        } else if (port == Integer.parseInt(allowedPort)) {
          return true;
        }
      } catch (NumberFormatException e) {
        LOG.warn("Ignoring firewall allowed port value '{}' due to parse error.", allowedPort, e);
      }
    }
    return false;
  }

  private Node getNode(Compute compute, Node.Type type, String zone, String nodeName) throws IOException {
    Instance instance;
    try {
      instance = compute.instances().get(conf.getProjectId(), zone, nodeName).execute();
    } catch (GoogleJsonResponseException e) {
      // this can happen right after a cluster is created
      if (e.getStatusCode() == 404) {
        return new Node(nodeName, Node.Type.UNKNOWN, "", -1L, Collections.emptyMap());
      }
      throw e;
    }
    Map<String, String> properties = new HashMap<>();

    // Dataproc cluster node should only have exactly one network
    instance.getNetworkInterfaces().stream().findFirst().ifPresent(networkInterface -> {
      // if the cluster does not have an external ip then then access config is null
      if (networkInterface.getAccessConfigs() != null) {
        for (AccessConfig accessConfig : networkInterface.getAccessConfigs()) {
          if (accessConfig.getNatIP() != null) {
            properties.put("ip.external", accessConfig.getNatIP());
            break;
          }
        }
      }
      properties.put("ip.internal", networkInterface.getNetworkIP());
    });

    long ts;
    try {
      ts = DATE_FORMAT.parse(instance.getCreationTimestamp()).getTime();
    } catch (ParseException e) {
      ts = -1L;
    }

    // For internal IP only cluster, nodes only have ip.internal.
    String ip = properties.get("ip.external");
    if (ip == null) {
      ip = properties.get("ip.internal");
    }
    return new Node(nodeName, type, ip, ts, properties);
  }

  /**
   * Converts Google Dataproc cluster status to CDAP Cluster Status
   */
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
  public void close() {
    client.close();
  }

  // if there was an API exception that was not a 4xx, we can just try again
  private RetryableProvisionException handleApiException(ApiException e) throws RetryableProvisionException {
    if (e.getStatusCode().getCode().getHttpStatusCode() / 100 != 4) {
      throw new RetryableProvisionException(e);
    }
    throw new DataprocRuntimeException(e);
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
