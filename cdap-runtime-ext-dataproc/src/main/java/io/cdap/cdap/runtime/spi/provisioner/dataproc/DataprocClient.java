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
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Network;
import com.google.api.services.compute.model.NetworkList;
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
import com.google.cloud.dataproc.v1.NodeInitializationAction;
import com.google.cloud.dataproc.v1.ShieldedInstanceConfig;
import com.google.cloud.dataproc.v1.SoftwareConfig;
import com.google.cloud.dataproc.v1.UpdateClusterRequest;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.longrunning.Operation;
import com.google.longrunning.OperationsClient;
import com.google.protobuf.Duration;
import com.google.protobuf.FieldMask;
import com.google.rpc.Status;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.provisioner.Node;
import io.cdap.cdap.runtime.spi.provisioner.RetryableProvisionException;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

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
  protected final ClusterControllerClient client;
  protected final Compute compute;

  protected DataprocClient(DataprocConf conf, ClusterControllerClient client, Compute compute) {
    this.conf = conf;
    this.client = client;
    this.compute = compute;
  }

  private String findNetwork(String project) throws IOException, RetryableProvisionException {
    List<Network> networks;
    try {
      NetworkList networkList = compute.networks().list(project).execute();
      networks = networkList.getItems();
    } catch (Exception e) {
      handleRetryableExceptions(e);
      throw e;
    }

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
                                         boolean privateInstance, @Nullable SSHPublicKey publicKey)
    throws RetryableProvisionException, InterruptedException, IOException {

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
        .addServiceAccountScopes(DataprocConf.CLOUD_PLATFORM_SCOPE)
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

      setNetworkConfigs(clusterConfig, privateInstance);

      Map<String, String> clusterProperties = new HashMap<>(conf.getClusterProperties());
      // Enable/Disable stackdriver
      clusterProperties.put("dataproc:dataproc.logging.stackdriver.enable",
                            Boolean.toString(conf.isStackdriverLoggingEnabled()));
      clusterProperties.put("dataproc:dataproc.monitoring.stackdriver.enable",
                            Boolean.toString(conf.isStackdriverMonitoringEnabled()));

      DiskConfig workerDiskConfig = DiskConfig.newBuilder()
        .setBootDiskSizeGb(conf.getWorkerDiskGB())
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
      if (Strings.isNullOrEmpty(conf.getAutoScalingPolicy()) && !conf.isPredefinedAutoScaleEnabled()) {
        //Set spark.default.parallelism according to cluster size.
        //Spark defaults it to number of current executors, but when we configure the job
        //executors may not have started yet, so this value gets artificially low.
        int defaultConcurrency = Math.max(conf.getTotalWorkerCPUs(), MIN_DEFAULT_CONCURRENCY);
        //Set spark.sql.adaptive.coalescePartitions.initialPartitionNum as 32x of default parallelism,
        //but no more than 8192. This value is used only in spark 3 with adaptive execution and
        //according to our tests spark can handle really large numbers and 32x is a reasonable default.
        int initialPartitionNum = Math.min(
          Math.max(conf.getTotalWorkerCPUs() * PARTITION_NUM_FACTOR, MIN_INITIAL_PARTITIONS_DEFAULT),
          MAX_INITIAL_PARTITIONS_DEFAULT);
        clusterProperties.putIfAbsent("spark:spark.default.parallelism", Integer.toString(defaultConcurrency));
        clusterProperties.putIfAbsent("spark:spark.sql.adaptive.coalescePartitions.initialPartitionNum",
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
                                            .setBootDiskSizeGb(conf.getMasterDiskGB())
                                            .setNumLocalSsds(0)
                                            .build())
                           .build())
        .setWorkerConfig(primaryWorkerConfig.build())
        .setSecondaryWorkerConfig(secondaryWorkerConfig.build())
        .setGceClusterConfig(clusterConfig.build())
        .setSoftwareConfig(softwareConfigBuilder);

      //Cluster TTL if one should be set
      if (conf.getIdleTTLMinutes() > 0) {
        long seconds = TimeUnit.MINUTES.toSeconds(conf.getIdleTTLMinutes());
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
        builder.setAutoscalingConfig(AutoscalingConfig.newBuilder().setPolicyUri(autoScalingPolicy).build());
      }

      if (conf.getEncryptionKeyName() != null) {
        builder.setEncryptionConfig(EncryptionConfig.newBuilder()
                                      .setGcePdKmsKeyName(conf.getEncryptionKeyName()).build());
      }

      if (conf.getGcsBucket() != null) {
        builder.setConfigBucket(conf.getGcsBucket());
      }

      Cluster cluster = com.google.cloud.dataproc.v1.Cluster.newBuilder()
        .setClusterName(name)
        .putAllLabels(labels)
        .setConfig(builder.build())
        .build();

      OperationFuture<Cluster, ClusterOperationMetadata> operationFuture =
        client.createClusterAsync(conf.getProjectId(), conf.getRegion(), cluster);
      return operationFuture.getMetadata().get();
    } catch (ExecutionException e) {
      cleanUpClusterAfterCreationFailure(name);
      Throwable cause = e.getCause();
      if (cause instanceof ApiException) {
        throw handleApiException((ApiException) cause);
      }
      throw new DataprocRuntimeException(cause);
    }
  }

  private void setNetworkConfigs(GceClusterConfig.Builder clusterConfig,
                                 boolean privateInstance) throws RetryableProvisionException, IOException {
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
      network = findNetwork(networkHostProjectId);
    }
    if (network == null) {
      throw new IllegalArgumentException("Unable to automatically detect a network, please explicitly set a network.");
    }
    setNetworkConfigs(clusterConfig, network, privateInstance);
  }

  protected abstract void setNetworkConfigs(GceClusterConfig.Builder clusterConfig, String network,
                                            boolean privateInstance)
    throws RetryableProvisionException, IOException;

  protected abstract Node getNode(Node.Type type, String zone, String nodeName) throws IOException;

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
      LOG.warn("Can't remove Dataproc Cluster " + name + ". " +
                 "Attempted deletion because state was ERROR after creation", e);
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
   */
  void updateClusterLabels(String clusterName,
                           Map<String, String> labelsToSet,
                           Collection<String> labelsToRemove) throws RetryableProvisionException, InterruptedException {
    if (labelsToSet.isEmpty() && labelsToRemove.isEmpty()) {
      return;
    }
    try {
      Cluster cluster = getDataprocCluster(clusterName)
        .filter(c -> c.getStatus().getState() == ClusterStatus.State.RUNNING)
        .orElseThrow(() -> new DataprocRuntimeException("Dataproc cluster " + clusterName +
                                                          " does not exist or not in running state"));
      Map<String, String> existingLabels = cluster.getLabelsMap();
      // If the labels to set are already exist and labels to remove are not set,
      // no need to update the cluster labelsToSet.
      if (labelsToSet.entrySet().stream().allMatch(e -> Objects.equals(e.getValue(), existingLabels.get(e.getKey()))) &&
          labelsToRemove.stream().noneMatch(existingLabels::containsKey)
      ) {
        return;
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
    return Optional.of(getCluster(cluster));
  }

  /**
   * Get image version for the specified cluster. This information will not be present if the cluster could not be
   * found, or the cluster specification doesn't contain this information
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
   * Converts dataproc cluster object into provisioner one. This version does not throw checked exceptions and
   * can be used as a {@link java.util.function.Function}.
   * @param cluster dataproc cluster object
   * @return provisioner cluster object
   * @throws DataprocRuntimeException if there was an error
   */
  private io.cdap.cdap.runtime.spi.provisioner.Cluster getClusterUnchecked(Cluster cluster) {
    try {
      return getCluster(cluster);
    } catch (IOException e) {
      throw new DataprocRuntimeException(e);
    }
  }

  /**
   * Converts dataproc cluster object into provisioner one.
   * @param cluster dataproc cluster object
   * @return provisioner cluster object
   * @throws IOException if there was an error
   */
  private io.cdap.cdap.runtime.spi.provisioner.Cluster getCluster(Cluster cluster) throws IOException {
    String zone = getZone(cluster.getConfig().getGceClusterConfig().getZoneUri());

    List<Node> nodes = new ArrayList<>();
    for (String masterName : cluster.getConfig().getMasterConfig().getInstanceNamesList()) {
      nodes.add(getNode(Node.Type.MASTER, zone, masterName));
    }
    for (String workerName : cluster.getConfig().getWorkerConfig().getInstanceNamesList()) {
      nodes.add(getNode(Node.Type.WORKER, zone, workerName));
    }
    io.cdap.cdap.runtime.spi.provisioner.Cluster c = new io.cdap.cdap.runtime.spi.provisioner.Cluster(
      cluster.getClusterName(), convertStatus(cluster.getStatus()), nodes, Collections.emptyMap());
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
      throw e;
    }
  }

  /**
   *
   * @param status if not null, return only clusters in the specified state
   * @param labels labels map to use as filters. A value can be "*" to indicate that label must be present
   *               with any value.
   * @return iterable list of clusters that conform to the filter
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  public Stream<io.cdap.cdap.runtime.spi.provisioner.Cluster> getClusters(
    @Nullable io.cdap.cdap.runtime.spi.provisioner.ClusterStatus status,
    Map<String, String> labels) throws RetryableProvisionException {
    return getClusters(status, labels, c -> true);
  }

  /**
   *
   * @param status if not null, return only clusters in the specified state
   * @param labels labels map to use as filters. A value can be "*" to indicate that label must be present
   *               with any value.
   * @param postFilter additional filter to apply before converting into provisioner cluster
   * @return iterable list of clusters that conform to the filter
   * @throws RetryableProvisionException if there was a non 4xx error code returned
   */
  public Stream<io.cdap.cdap.runtime.spi.provisioner.Cluster> getClusters(
    @Nullable io.cdap.cdap.runtime.spi.provisioner.ClusterStatus status,
    Map<String, String> labels,
    Predicate<Cluster> postFilter) throws RetryableProvisionException {

    try {
      String filter = Stream.concat(
        Optional.ofNullable(status).map(s -> Stream.of(String.format("status.state=%s", s))).orElse(Stream.empty()),
        labels.entrySet().stream().map(e -> String.format("labels.%s=%s", e.getKey(), e.getValue())))
        .collect(Collectors.joining(" AND "));
      return StreamSupport.stream(
        client.listClusters(conf.getProjectId(), conf.getRegion(), filter).iterateAll().spliterator(), false)
        .filter(postFilter)
        .map(this::getClusterUnchecked);
    } catch (ApiException e) {
      throw handleApiException(e);
    }
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

  //Throws retryable Exception for the cases that are transient in nature
  protected void handleRetryableExceptions(Exception e) throws RetryableProvisionException {
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

      if (statusCode == HttpURLConnection.HTTP_FORBIDDEN || statusCode == DataprocUtils.RESOURCE_EXHAUSTED) {
        boolean isRetryAble = gError.getDetails().getErrors().stream()
          .anyMatch(errorInfo -> ERROR_INFO_REASONS.contains(errorInfo.getReason()));
        if (isRetryAble) {
          throw new RetryableProvisionException(e);
        }
      }
    }
  }
}
