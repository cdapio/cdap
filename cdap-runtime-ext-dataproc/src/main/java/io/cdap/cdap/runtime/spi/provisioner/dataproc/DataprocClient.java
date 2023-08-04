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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Network;
import com.google.api.services.compute.model.NetworkList;
import com.google.api.services.compute.model.NetworkPeering;
import com.google.cloud.dataproc.v1.AutoscalingConfig;
import com.google.cloud.dataproc.v1.Cluster;
import com.google.cloud.dataproc.v1.ClusterConfig;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.ClusterOperationMetadata;
import com.google.cloud.dataproc.v1.ClusterStatus;
import com.google.cloud.dataproc.v1.DeleteClusterRequest;
import com.google.cloud.dataproc.v1.DiskConfig;
import com.google.cloud.dataproc.v1.EncryptionConfig;
import com.google.cloud.dataproc.v1.EndpointConfig;
import com.google.cloud.dataproc.v1.GceClusterConfig;
import com.google.cloud.dataproc.v1.GetClusterRequest;
import com.google.cloud.dataproc.v1.InstanceGroupConfig;
import com.google.cloud.dataproc.v1.LifecycleConfig;
import com.google.cloud.dataproc.v1.ListClustersRequest;
import com.google.cloud.dataproc.v1.NodeInitializationAction;
import com.google.cloud.dataproc.v1.ShieldedInstanceConfig;
import com.google.cloud.dataproc.v1.SoftwareConfig;
import com.google.cloud.dataproc.v1.UpdateClusterRequest;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.longrunning.Operation;
import com.google.longrunning.OperationsClient;
import com.google.protobuf.Duration;
import com.google.protobuf.Empty;
import com.google.protobuf.FieldMask;
import com.google.rpc.Status;
import io.cdap.cdap.error.api.ErrorTagProvider.ErrorTag;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.provisioner.Node;
import io.cdap.cdap.runtime.spi.provisioner.RetryableProvisionException;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.security.GeneralSecurityException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around the dataproc client that adheres to our configuration settings.
 */
abstract class DataprocClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(DataprocClient.class);
  private static final int MIN_DEFAULT_CONCURRENCY = 32;
  private static final int PARTITION_NUM_FACTOR = 32;
  private static final int MIN_INITIAL_PARTITIONS_DEFAULT = 128;
  private static final int MAX_INITIAL_PARTITIONS_DEFAULT = 8192;
  private static final Set<String> ERROR_INFO_REASONS = ImmutableSet.of(
      "rateLimitExceeded",
      "resourceQuotaExceeded");
  protected final DataprocConf conf;
  private final ClusterControllerClient client;
  private final ComputeFactory computeFactory;
  private Compute compute;

  protected DataprocClient(DataprocConf conf, ClusterControllerClient client,
      ComputeFactory computeFactory) {
    this.conf = conf;
    this.client = client;
    this.computeFactory = computeFactory;
  }

  private static String findNetwork(Compute compute, String project)
      throws IOException, RetryableProvisionException {
    List<Network> networks;
    try {
      NetworkList networkList = compute.networks().list(project).execute();
      networks = networkList.getItems();
    } catch (Exception e) {
      handleRetryableExceptions(e);
      throw new DataprocRuntimeException(e);
    }

    if (networks == null || networks.isEmpty()) {
      throw new DataprocRuntimeException(
          String.format(
              "Unable to find any networks in project '%s'. Please create a network in the project.",
              project),
          ErrorTag.CONFIGURATION);
    }

    for (Network network : networks) {
      if ("default".equals(network.getName())) {
        return network.getName();
      }
    }

    return networks.iterator().next().getName();
  }

  /**
   * Extracts and returns the zone name from the given full zone URI.
   */
  private static String getZone(String zoneUri) {
    int idx = zoneUri.lastIndexOf("/");
    if (idx <= 0) {
      throw new DataprocRuntimeException("Invalid zone uri " + zoneUri, ErrorTag.CONFIGURATION);
    }
    return zoneUri.substring(idx + 1);
  }

  /**
   * Create a cluster. This will return after the initial request to create the cluster is
   * completed. At this point, the cluster is likely not yet running, but in a provisioning state.
   *
   * @param name the name of the cluster to create
   * @param imageVersion the image version for the cluster
   * @param labels labels to set on the cluster
   * @param privateInstance {@code true} to indicate using private instance
   * @param publicKey the public key to set for cluster nodes if SSH access is required
   * @return create operation metadata
   * @throws InterruptedException if the thread was interrupted while waiting for the initial
   *     request to complete
   * @throws AlreadyExistsException if the cluster already exists
   * @throws IOException if there was an I/O error talking to Google Compute APIs
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  ClusterOperationMetadata createCluster(String name, String imageVersion,
      Map<String, String> labels,
      boolean privateInstance, @Nullable SSHPublicKey publicKey)
      throws RetryableProvisionException, InterruptedException, IOException {

    String operationId = null;
    try {
      Map<String, String> metadata = new HashMap<>();
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
          .addAllServiceAccountScopes(conf.getScopes())
          .setShieldedInstanceConfig(
              ShieldedInstanceConfig.newBuilder()
                  .setEnableSecureBoot(conf.isSecureBootEnabled())
                  .setEnableVtpm(conf.isvTpmEnabled())
                  .setEnableIntegrityMonitoring(conf.isIntegrityMonitoringEnabled()).build())
          .putAllMetadata(metadata);

      if (conf.getServiceAccount() != null) {
        clusterConfig.setServiceAccount(conf.getServiceAccount());
      }

      if (conf.getZone() != null) {
        clusterConfig.setZoneUri(conf.getZone());
      }

      Compute compute = getOrCreateCompute();
      setNetworkConfigs(compute, clusterConfig, privateInstance);

      Map<String, String> clusterProperties = new HashMap<>(conf.getClusterProperties());
      // Enable/Disable stackdriver
      clusterProperties.put("dataproc:dataproc.logging.stackdriver.enable",
          Boolean.toString(conf.isStackdriverLoggingEnabled()));
      clusterProperties.put("dataproc:dataproc.monitoring.stackdriver.enable",
          Boolean.toString(conf.isStackdriverMonitoringEnabled()));

      DiskConfig workerDiskConfig = DiskConfig.newBuilder()
          .setBootDiskSizeGb(conf.getWorkerDiskGb())
          .setBootDiskType(conf.getWorkerDiskType())
          .setNumLocalSsds(0)
          .build();
      InstanceGroupConfig.Builder primaryWorkerConfig = InstanceGroupConfig.newBuilder()
          .setNumInstances(conf.getWorkerNumNodes())
          .setMachineTypeUri(conf.getWorkerMachineType())
          .setDiskConfig(workerDiskConfig);
      InstanceGroupConfig.Builder secondaryWorkerConfig = InstanceGroupConfig.newBuilder()
          .setNumInstances(conf.getSecondaryWorkerNumNodes())
          .setMachineTypeUri(conf.getWorkerMachineType())
          .setPreemptibility(InstanceGroupConfig.Preemptibility.NON_PREEMPTIBLE)
          .setDiskConfig(workerDiskConfig);

      //Set default concurrency settings for fixed cluster
      if (Strings.isNullOrEmpty(conf.getAutoScalingPolicy())
          && !conf.isPredefinedAutoScaleEnabled()) {
        //Set spark.default.parallelism according to cluster size.
        //Spark defaults it to number of current executors, but when we configure the job
        //executors may not have started yet, so this value gets artificially low.
        int defaultConcurrency = Math.max(conf.getTotalWorkerCpus(), MIN_DEFAULT_CONCURRENCY);
        //Set spark.sql.adaptive.coalescePartitions.initialPartitionNum as 32x of default parallelism,
        //but no more than 8192. This value is used only in spark 3 with adaptive execution and
        //according to our tests spark can handle really large numbers and 32x is a reasonable default.
        int initialPartitionNum = Math.min(
            Math.max(conf.getTotalWorkerCpus() * PARTITION_NUM_FACTOR,
                MIN_INITIAL_PARTITIONS_DEFAULT),
            MAX_INITIAL_PARTITIONS_DEFAULT);
        clusterProperties.putIfAbsent("spark:spark.default.parallelism",
            Integer.toString(defaultConcurrency));
        clusterProperties.putIfAbsent(
            "spark:spark.sql.adaptive.coalescePartitions.initialPartitionNum",
            Integer.toString(initialPartitionNum));
      }

      SoftwareConfig.Builder softwareConfigBuilder = SoftwareConfig.newBuilder()
          .putAllProperties(clusterProperties);
      //Use image version only if custom Image URI is not specified, otherwise may cause image version conflicts
      if (conf.getCustomImageUri() == null || conf.getCustomImageUri().isEmpty()) {
        softwareConfigBuilder.setImageVersion(imageVersion);
      } else {
        //If custom Image URI is specified, use that for cluster creation
        primaryWorkerConfig.setImageUri(conf.getCustomImageUri());
        secondaryWorkerConfig.setImageUri(conf.getCustomImageUri());
      }

      ClusterConfig.Builder builder = ClusterConfig.newBuilder()
          .setEndpointConfig(EndpointConfig.newBuilder()
              .setEnableHttpPortAccess(conf.isComponentGatewayEnabled())
              .build())
          .setMasterConfig(InstanceGroupConfig.newBuilder()
              .setNumInstances(conf.getMasterNumNodes())
              .setMachineTypeUri(conf.getMasterMachineType())
              .setDiskConfig(DiskConfig.newBuilder()
                  .setBootDiskType(conf.getMasterDiskType())
                  .setBootDiskSizeGb(conf.getMasterDiskGb())
                  .setNumLocalSsds(0)
                  .build())
              .build())
          .setWorkerConfig(primaryWorkerConfig.build())
          .setSecondaryWorkerConfig(secondaryWorkerConfig.build())
          .setGceClusterConfig(clusterConfig.build())
          .setSoftwareConfig(softwareConfigBuilder);

      //Cluster TTL if one should be set
      if (conf.getIdleTtlMinutes() > 0) {
        long seconds = TimeUnit.MINUTES.toSeconds(conf.getIdleTtlMinutes());
        builder.setLifecycleConfig(LifecycleConfig.newBuilder()
            .setIdleDeleteTtl(Duration.newBuilder().setSeconds(seconds).build()).build());
      }

      //Add any Node Initialization action scripts
      for (String action : conf.getInitActions()) {
        builder.addInitializationActions(
            NodeInitializationAction.newBuilder()
                .setExecutableFile(action)
                .build());
      }

      // Set Auto Scaling Policy
      String autoScalingPolicy = conf.getAutoScalingPolicy();
      if (conf.isPredefinedAutoScaleEnabled()) {
        PredefinedAutoScaling predefinedAutoScaling = new PredefinedAutoScaling(conf);
        autoScalingPolicy = predefinedAutoScaling.createPredefinedAutoScalingPolicy();
      }

      if (!Strings.isNullOrEmpty(autoScalingPolicy)) {
        //Check if policy is URI or ID. If ID Convert to URI
        if (!autoScalingPolicy.contains("/")) {
          autoScalingPolicy = "projects/" + conf.getProjectId() + "/regions/" + conf.getRegion()
              + "/autoscalingPolicies/" + autoScalingPolicy;
        }
        builder.setAutoscalingConfig(
            AutoscalingConfig.newBuilder().setPolicyUri(autoScalingPolicy).build());
      }

      if (conf.getEncryptionKeyName() != null) {
        builder.setEncryptionConfig(EncryptionConfig.newBuilder()
            .setGcePdKmsKeyName(conf.getEncryptionKeyName()).build());
      }

      if (!Strings.isNullOrEmpty(conf.getGcsBucket())) {
        builder.setConfigBucket(conf.getGcsBucket());
      }

      if (!Strings.isNullOrEmpty(conf.getTempBucket())) {
        builder.setTempBucket(conf.getTempBucket());
      }

      Cluster cluster = com.google.cloud.dataproc.v1.Cluster.newBuilder()
          .setClusterName(name)
          .putAllLabels(labels)
          .setConfig(builder.build())
          .build();

      OperationFuture<Cluster, ClusterOperationMetadata> operationFuture =
          client.createClusterAsync(conf.getProjectId(), conf.getRegion(), cluster);
      operationId = operationFuture.getName();
      return operationFuture.getMetadata().get();
    } catch (ExecutionException e) {
      cleanUpClusterAfterCreationFailure(name);
      Throwable cause = e.getCause();
      if (cause instanceof ApiException) {
        throw handleApiException(operationId, (ApiException) cause);
      }
      throw new DataprocRuntimeException(operationId, cause);
    }
  }

  protected void setNetworkConfigs(Compute compute, GceClusterConfig.Builder clusterConfig,
      boolean privateInstance) throws RetryableProvisionException, IOException {
    String network = conf.getNetwork();
    String systemNetwork = null;
    try {
      systemNetwork = DataprocUtils.getSystemNetwork();
    } catch (IllegalArgumentException e) {
      // expected when not running on GCP, ignore
    }

    String projectId = conf.getProjectId();
    String networkHostProjectId = conf.getNetworkHostProjectId();
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
      network = findNetwork(compute, networkHostProjectId);
    }
    if (network == null) {
      throw new DataprocRuntimeException(
          "Unable to automatically detect a network, please explicitly set a network.",
          ErrorTag.CONFIGURATION);
    }

    Network networkInfo = getNetworkInfo(networkHostProjectId, network, compute);

    String subnet = conf.getSubnet();
    List<String> subnets = networkInfo.getSubnetworks();
    if (subnet != null && !subnetExists(subnets, subnet)) {
      throw new DataprocRuntimeException(
          String.format(
              "Subnet '%s' does not exist in network '%s' in project '%s'. Please use a different subnet.",
              subnet, network, networkHostProjectId),
          ErrorTag.CONFIGURATION);
    }

    // if the network uses custom subnets, a subnet must be provided to the dataproc api
    boolean autoCreateSubnet =
        networkInfo.getAutoCreateSubnetworks() != null && networkInfo.getAutoCreateSubnetworks();
    if (!autoCreateSubnet) {
      // if the network uses custom subnets but none exist, error out
      if (subnets == null || subnets.isEmpty()) {
        throw new DataprocRuntimeException(
            String.format("Network '%s' in project '%s' does not contain any subnets. "
                    + "Please create a subnet or use a different network.", network,
                networkHostProjectId),
            ErrorTag.CONFIGURATION);
      }
    }

    subnet = chooseSubnet(network, subnets, subnet, conf.getRegion());

    // subnets are unique within a location, not within a network, which is why these configs are mutually exclusive.
    clusterConfig.setSubnetworkUri(subnet);

    //Add any defined Network Tags
    clusterConfig.addAllTags(conf.getNetworkTags());
    boolean isInternalIpOnly = isInternalIpOnly(networkInfo, privateInstance);
    clusterConfig.setInternalIpOnly(isInternalIpOnly);
    addNetworkTags(clusterConfig, networkInfo, isInternalIpOnly);
  }

  protected void addNetworkTags(GceClusterConfig.Builder clusterConfig, Network network,
      boolean internalIpOnly)
      throws RetryableProvisionException, IOException {
    // no-op
  }

  protected abstract Node getNode(Node.Type type, String zone, String nodeName) throws IOException;

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
  private static String chooseSubnet(String network, List<String> subnets, @Nullable String subnet,
      String region) {
    for (String currentSubnet : subnets) {
      // if a subnet name is given then get the region of that subnet based on the zone
      if (subnet != null && !currentSubnet.endsWith("subnetworks/" + subnet)) {
        continue;
      }
      if (currentSubnet.contains(region + "/subnetworks")) {
        return currentSubnet;
      }
    }
    throw new DataprocRuntimeException(
        String.format("Could not find %s in network '%s' that are for region '%s'",
            subnet == null ? "any subnet" :
                String.format("a subnet named '%s", subnet), network, region),
        ErrorTag.CONFIGURATION);
  }

  private Network getNetworkInfo(String project, String network, Compute compute)
      throws IOException, RetryableProvisionException {
    Network networkObj;
    try {
      networkObj = compute.networks().get(project, network).execute();
    } catch (Exception e) {
      handleRetryableExceptions(e);
      throw new DataprocRuntimeException(e);
    }

    if (networkObj == null) {
      throw new DataprocRuntimeException(
          String.format(
              "Unable to find network '%s' in project '%s'. Please specify another network.",
              network, project),
          ErrorTag.CONFIGURATION
      );
    }
    return networkObj;
  }

  /**
   * Determines if the Dataproc cluster is private IP only.
   *
   * @param privateInstance a system config to force using private instance
   * @return {@code true} for pribvate IP only Dataproc cluster
   */
  private boolean isInternalIpOnly(Network network, boolean privateInstance) {
    String systemProjectId = null;
    String systemNetwork = null;
    try {
      systemProjectId = DataprocUtils.getSystemProjectId();
      systemNetwork = DataprocUtils.getSystemNetwork();
    } catch (IllegalArgumentException e) {
      // expected when not running on GCP, ignore
    }

    // Use private IP only cluster if privateInstance is true or if the compute profile required
    if (!privateInstance && conf.isPreferExternalIp()) {
      return false;
    }

    // If it is forced to be private instance or
    // if CDAP runs in GCP project and runtime job manager is used and monitoring is not done through SSH,
    // then we don't need to validate network connectivity
    if (privateInstance) {
      return true;
    }

    // If the CDAP is not running on GCP VM, then we just honor the prefer external IP config
    if (systemProjectId == null || systemNetwork == null) {
      return true;
    }

    // SSH will be used for job launching and/or monitoring, we need to validate network connectivity
    // CDAP and Dataproc are in the same network, should be able to use private IP only cluster
    if (systemProjectId.equals(conf.getNetworkHostProjectId()) && systemNetwork.equals(
        network.getName())) {
      return true;
    }

    // Check network is peering, we can use private ip only cluster
    PeeringState state = getPeeringState(systemProjectId, systemNetwork, network);
    if (state == PeeringState.ACTIVE) {
      return true;
    }

    // If there is no network connectivity and yet private ip only cluster is requested, raise an exception
    throw new DataprocRuntimeException(
        String.format(
            "Direct network connectivity is needed for private Dataproc cluster between VPC %s/%s and %s/%s",
            systemProjectId, systemNetwork, conf.getNetworkHostProjectId(), network.getName()),
        ErrorTag.CONFIGURATION);
  }

  private static PeeringState getPeeringState(String systemProjectId, String systemNetwork,
      Network networkInfo) {
    // note: vpc network is a global resource.
    // https://cloud.google.com/compute/docs/regions-zones/global-regional-zonal-resources#globalresources
    String systemNetworkPath = String.format(
        "https://www.googleapis.com/compute/v1/projects/%s/global/networks/%s",
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

  private void cleanUpClusterAfterCreationFailure(String name) {
    if (conf.isSkipDelete()) {
      //Don't delete even failed one when skip delete is set
      return;
    }
    try {
      Optional<Cluster> cluster = getDataprocCluster(name)
          .filter(c -> c.getStatus().getState() == ClusterStatus.State.ERROR);
      if (cluster.isPresent()) {
        deleteCluster(name);
      }
    } catch (Exception e) {
      LOG.warn("Can't remove Dataproc Cluster " + name + ". "
          + "Attempted deletion because state was ERROR after creation", e);
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
    updateClusterLabels(clusterName, labels, Collections.emptyList());
  }

  /**
   * Updates labelsToSet on the given Dataproc cluster.
   *
   * @param clusterName name of the cluster
   * @param labelsToSet Key/Value pairs to set on the Dataproc cluster.
   * @param labelsToRemove collection of labels to remove from the Dataproc cluster.
   *
   * @return future that would complete after label update is finished
   */
  Future<?> updateClusterLabels(String clusterName,
      Map<String, String> labelsToSet,
      Collection<String> labelsToRemove) throws RetryableProvisionException, InterruptedException {
    if (labelsToSet.isEmpty() && labelsToRemove.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    try {
      Cluster cluster = getDataprocCluster(clusterName)
          .filter(c -> c.getStatus().getState() == ClusterStatus.State.RUNNING)
          .orElseThrow(() -> new DataprocRuntimeException(
              "Dataproc cluster " + clusterName + " does not exist or not in running state",
              ErrorTag.CONFIGURATION));
      Map<String, String> existingLabels = cluster.getLabelsMap();
      // If the labels to set are already exist and labels to remove are not set,
      // no need to update the cluster labelsToSet.
      if (labelsToSet.entrySet().stream()
          .allMatch(e -> Objects.equals(e.getValue(), existingLabels.get(e.getKey())))
          && labelsToRemove.stream().noneMatch(existingLabels::containsKey)
      ) {
        return CompletableFuture.completedFuture(null);
      }
      Map<String, String> newLabels = new HashMap<>(existingLabels);
      newLabels.keySet().removeAll(labelsToRemove);
      newLabels.putAll(labelsToSet);

      FieldMask updateMask = FieldMask.newBuilder().addPaths("labels").build();
      OperationFuture<Cluster, ClusterOperationMetadata> operationFuture =
          client.updateClusterAsync(UpdateClusterRequest
              .newBuilder()
              .setProjectId(conf.getProjectId())
              .setRegion(conf.getRegion())
              .setClusterName(clusterName)
              .setCluster(cluster.toBuilder().clearLabels().putAllLabels(newLabels))
              .setUpdateMask(updateMask)
              .build());

      ClusterOperationMetadata metadata = operationFuture.getMetadata().get();
      int numWarnings = metadata.getWarningsCount();
      if (numWarnings > 0) {
        LOG.warn("Encountered {} warning {} while setting labels on cluster:\n{}",
            numWarnings, numWarnings > 1 ? "s" : "", String.join("\n", metadata.getWarningsList()));
      }
      return operationFuture;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ApiException) {
        throw handleApiException((ApiException) cause);
      }
      throw new DataprocRuntimeException(cause);
    }
  }

  /**
   * Delete the specified cluster if it exists. This will return after the initial request to delete
   * the cluster is completed. At this point, the cluster is likely not yet deleted, but in a
   * deleting state.
   *
   * @param name the name of the cluster to delete
   * @throws InterruptedException if the thread was interrupted while waiting for the initial
   *     request to complete
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  Optional<ClusterOperationMetadata> deleteCluster(String name)
      throws RetryableProvisionException, InterruptedException {
    String operationId = null;
    try {
      DeleteClusterRequest request = DeleteClusterRequest.newBuilder()
          .setClusterName(name)
          .setProjectId(conf.getProjectId())
          .setRegion(conf.getRegion())
          .build();
      OperationFuture<Empty, ClusterOperationMetadata> operationFuture = client.deleteClusterAsync(
          request);
      operationId = operationFuture.getName();
      return Optional.of(operationFuture.getMetadata().get());
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
          if (!cluster.isPresent()
              || cluster.get().getStatus().getState() == ClusterStatus.State.DELETING) {
            return Optional.empty();
          }
          // if the cluster isn't gone or in the deleting state, handle the original delete error
        } catch (Exception e1) {
          // if there was an error getting the cluster information, ignore it and handle the original delete error
        }
        throw handleApiException(operationId, (ApiException) cause);
      }
      throw new DataprocRuntimeException(operationId, cause);
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

    return status;
  }

  String getClusterFailureMsg(String name) throws RetryableProvisionException {
    // TODO: this can return multiple operations if the cluster name is the same (ex: a previous cluster had the
    // same name, was deleted, then this current cluster was created). This won't happen in practice for ephemeral
    // clusters, but it's still not great to have this possibility in the implementation.
    // https://cdap.atlassian.net/browse/CDAP-19641
    String resourceName = String.format("projects/%s/regions/%s/operations", conf.getProjectId(),
        conf.getRegion());
    String filter = String.format("clusterName=%s AND operationType=CREATE", name);

    OperationsClient.ListOperationsPagedResponse operationsResponse;
    try {
      operationsResponse = client.getOperationsClient().listOperations(resourceName, filter);
    } catch (ApiException e) {
      if (e.getStatusCode().getCode().getHttpStatusCode() / 100 != 4) {
        // if there was an API exception that was not a 4xx, we can just try again
        throw new RetryableProvisionException(e);
      }
      // otherwise, it's not a retryable failure
      throw e;
    }

    OperationsClient.ListOperationsPage page = operationsResponse.getPage();
    if (page == null) {
      return "Unable to get the cause of the cluster creation failure.";
    }

    if (page.getPageElementCount() > 1) {
      // shouldn't be possible
      return MessageFormat.format(
          "Multiple create operations found for cluster {0}, may not be able to "
              + "find the failure message.", name);
    }

    if (page.getPageElementCount() > 0) {
      Operation operation = page.getValues().iterator().next();
      Status operationError = operation.getError();
      if (operationError != null) {
        return MessageFormat.format("Failed to create cluster {0}: {1}. Details: {2}", name,
            operationError.getMessage(),
            operationError.getDetailsList() != null
                ? Arrays.toString(operationError.getDetailsList().toArray()) : "");
      }
    }

    return "";
  }

  /**
   * Get information about the specified cluster. The cluster will not be present if it could not be
   * found.
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
    return Optional.of(convertCluster(cluster));
  }

  /**
   * Get image version for the specified cluster. This information will not be present if the
   * cluster could not be found, or the cluster specification doesn't contain this information
   *
   * @param name the cluster name
   * @return the cluster image version if available.
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  Optional<String> getClusterImageVersion(String name)
      throws RetryableProvisionException {
    Optional<Cluster> clusterOptional = getDataprocCluster(name);
    if (!clusterOptional.isPresent()) {
      return Optional.empty();
    }

    Cluster cluster = clusterOptional.get();
    SoftwareConfig softwareConfig = cluster.getConfig().getSoftwareConfig();
    return Optional.ofNullable(softwareConfig.getImageVersion());
  }

  /**
   * Converts dataproc cluster object into provisioner one. This version does not throw checked
   * exceptions and can be used as a {@link java.util.function.Function}.
   *
   * @param cluster dataproc cluster object
   * @return provisioner cluster object
   * @throws DataprocRuntimeException if there was an error
   */
  private io.cdap.cdap.runtime.spi.provisioner.Cluster convertClusterUnchecked(Cluster cluster) {
    try {
      return convertCluster(cluster);
    } catch (IOException e) {
      throw new DataprocRuntimeException(e);
    }
  }

  /**
   * Converts dataproc cluster object into provisioner one.
   *
   * @param cluster dataproc cluster object
   * @return provisioner cluster object
   * @throws IOException if there was an error
   */
  private io.cdap.cdap.runtime.spi.provisioner.Cluster convertCluster(Cluster cluster)
      throws IOException {
    String zone = getZone(cluster.getConfig().getGceClusterConfig().getZoneUri());

    List<Node> nodes = new ArrayList<>();
    for (String masterName : cluster.getConfig().getMasterConfig().getInstanceNamesList()) {
      nodes.add(getNode(Node.Type.MASTER, zone, masterName));
    }
    for (String workerName : cluster.getConfig().getWorkerConfig().getInstanceNamesList()) {
      nodes.add(getNode(Node.Type.WORKER, zone, workerName));
    }
    io.cdap.cdap.runtime.spi.provisioner.Cluster c = new io.cdap.cdap.runtime.spi.provisioner.Cluster(
        cluster.getClusterName(), convertStatus(cluster.getStatus()), nodes,
        Collections.emptyMap());
    return c;
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
      throw new DataprocRuntimeException(e);
    }
  }

  /**
   * Retrieve clusters filtering by labels.
   *
   * @param labels labels map to use as filters. A value can be "*" to indicate that label must
   *     be present with any value.
   * @return iterable list of clusters that conform to the filter
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  public Stream<io.cdap.cdap.runtime.spi.provisioner.Cluster> getClusters(
      Map<String, String> labels) throws RetryableProvisionException {
    return getClusters(labels, c -> true);
  }

  /**
   * Retrieve clusters filtering by labels and postFilter.
   *
   * @param labels labels map to use as filters. A value can be "*" to indicate that label must
   *     be present with any value.
   * @param postFilter additional filter to apply before converting into provisioner cluster
   * @return iterable list of clusters that conform to the filter
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  public Stream<io.cdap.cdap.runtime.spi.provisioner.Cluster> getClusters(
      Map<String, String> labels,
      Predicate<Cluster> postFilter) throws RetryableProvisionException {

    try {
      ListClustersRequest.Builder builder = ListClustersRequest.newBuilder()
          .setProjectId(conf.getProjectId())
          .setRegion(conf.getRegion());
      if (!labels.isEmpty()) {
        builder.setFilter(labels.entrySet().stream()
            .map(e -> String.format("labels.%s=%s", e.getKey(), e.getValue()))
            .collect(Collectors.joining(" AND ")));
      }
      return StreamSupport.stream(
              client.listClusters(builder.build()).iterateAll().spliterator(), false)
          .filter(postFilter)
          .map(this::convertClusterUnchecked);
    } catch (ApiException e) {
      throw handleApiException(e);
    }
  }

  /**
   * Converts Google Dataproc cluster status to CDAP Cluster Status.
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

  protected Compute getOrCreateCompute() throws IOException {
    if (compute == null) {
      try {
        compute = computeFactory.createCompute(conf);
      } catch (GeneralSecurityException e) {
        throw new IOException("Unable to load compute credentials: " + e.getMessage(), e);
      }
    }
    return compute;
  }

  @Override
  public void close() {
    client.close();
  }

  // if there was an API exception that was not a 4xx, we can just try again
  protected RetryableProvisionException handleApiException(ApiException e)
      throws RetryableProvisionException {
    return handleApiException(null, e);
  }

  private RetryableProvisionException handleApiException(@Nullable String operationId,
      ApiException e)
      throws RetryableProvisionException {
    if (e.getStatusCode().getCode().getHttpStatusCode() / 100 != 4) {
      throw new DataprocRetryableException(operationId, e);
    }
    String helpMessage = DataprocUtils.getTroubleshootingHelpMessage(
        conf.getTroubleshootingDocsUrl());
    if (e instanceof InvalidArgumentException) {
      throw new DataprocRuntimeException(operationId, helpMessage, e, ErrorTag.USER);
    }
    throw new DataprocRuntimeException(operationId, helpMessage, e);
  }

  //Throws retryable Exception for the cases that are transient in nature
  protected static void handleRetryableExceptions(Exception e) throws RetryableProvisionException {
    // if there was an SocketTimeoutException ( read time out ) , we can just try again
    if (e instanceof SocketTimeoutException) {
      throw new RetryableProvisionException(e);
    }

    //Attempt retry in case of rate limit errors and service unavailable
    if (e instanceof GoogleJsonResponseException) {
      GoogleJsonResponseException gError = ((GoogleJsonResponseException) e);
      int statusCode = gError.getStatusCode();

      if (statusCode == HttpURLConnection.HTTP_UNAVAILABLE) {
        throw new RetryableProvisionException(e);
      }

      if (statusCode == HttpURLConnection.HTTP_FORBIDDEN
          || statusCode == DataprocUtils.RESOURCE_EXHAUSTED) {
        boolean isRetryAble = gError.getDetails().getErrors().stream()
            .anyMatch(errorInfo -> ERROR_INFO_REASONS.contains(errorInfo.getReason()));
        if (isRetryAble) {
          throw new RetryableProvisionException(e);
        }
      }
    }
  }

  private enum PeeringState {
    ACTIVE,
    INACTIVE,
    NONE
  }
}
