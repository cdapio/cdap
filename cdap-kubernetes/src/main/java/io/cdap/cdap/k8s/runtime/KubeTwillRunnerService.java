/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.k8s.runtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.k8s.common.AbstractWatcherThread;
import io.cdap.cdap.k8s.common.ResourceChangeListener;
import io.cdap.cdap.k8s.util.KubeUtil;
import io.cdap.cdap.k8s.util.WorkloadIdentityUtil;
import io.cdap.cdap.master.environment.k8s.ApiClientFactory;
import io.cdap.cdap.master.environment.k8s.KubeMasterEnvironment;
import io.cdap.cdap.master.environment.k8s.PodInfo;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.namespace.NamespaceDetail;
import io.cdap.cdap.master.spi.namespace.NamespaceListener;
import io.cdap.cdap.master.spi.twill.ExtendedTwillApplication;
import io.cdap.cdap.proto.id.NamespaceId;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.V1ClusterRoleBinding;
import io.kubernetes.client.openapi.models.V1ClusterRoleBindingBuilder;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobStatus;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1ResourceQuota;
import io.kubernetes.client.openapi.models.V1ResourceQuotaSpec;
import io.kubernetes.client.openapi.models.V1RoleBinding;
import io.kubernetes.client.openapi.models.V1RoleBindingBuilder;
import io.kubernetes.client.openapi.models.V1RoleRefBuilder;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1ServiceAccount;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1SubjectBuilder;
import io.kubernetes.client.openapi.models.V1Volume;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kubernetes version of a TwillRunner.
 *
 * All resources created by the runner will have a name of the form:
 *
 * [cleansed app name]-[hash]
 *
 * Each resource will also have the following labels:
 *
 * cdap.twill.runner=k8s cdap.twill.run.id=[run id] cdap.twill.app=[cleansed app name]
 *
 * and annotations:
 *
 * cdap.twill.app=[literal app name]
 */
public class KubeTwillRunnerService implements TwillRunnerService, NamespaceListener {

  static final String START_TIMEOUT_ANNOTATION = "cdap.app.start.timeout.millis";

  private static final Logger LOG = LoggerFactory.getLogger(KubeTwillRunnerService.class);

  public static final String APP_LABEL = "cdap.twill.app";
  private static final String CDAP_NAMESPACE_LABEL = "cdap.namespace";
  private static final String NAMESPACE_CPU_LIMIT_PROPERTY = "k8s.namespace.cpu.limits";
  private static final String NAMESPACE_MEMORY_LIMIT_PROPERTY = "k8s.namespace.memory.limits";
  public static final String RUN_ID_LABEL = "cdap.twill.run.id";
  private static final String RUNNER_LABEL = "cdap.twill.runner";
  private static final String RUNNER_LABEL_VAL = "k8s";
  private static final String WORKLOAD_LAUNCHER_NAMESPACE_ROLE_BINDING_NAME
      = "cdap-workload-launcher-namespace-role-binding";
  private static final String WORKLOAD_LAUNCHER_CLUSTER_ROLE_BINDING_FORMAT
      = "cdap-workload-launcher-cluster-role-binding-%s";
  private static final String RBAC_V1_API_GROUP = "rbac.authorization.k8s.io";
  private static final String CLUSTER_ROLE_KIND = "ClusterRole";
  private static final String SERVICE_ACCOUNT_KIND = "ServiceAccount";
  public static final String RESOURCE_QUOTA_NAME = "cdap-resource-quota";
  public static final String WORKLOAD_IDENTITY_GCP_SERVICE_ACCOUNT_EMAIL_PROPERTY =
      "workload.identity.gcp.service.account.email";
  // Whether to cleanup resources after job completion
  public static final String RUNTIME_CLEANUP_DISABLED = "system.runtime.cleanup.disabled";

  private final MasterEnvironmentContext masterEnvContext;
  private final ApiClientFactory apiClientFactory;
  private final String kubeNamespace;
  private final String resourcePrefix;
  private final PodInfo podInfo;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final Map<String, String> extraLabels;
  private final Map<String, Map<Type, AppResourceWatcherThread<?>>> resourceWatchers;
  private final Map<String, KubeLiveInfo> liveInfos;
  private final Lock liveInfoLock;
  private final String selector;
  private final boolean enableMonitor;
  private final String workloadLauncherRoleNameForNamespace;
  private final String workloadLauncherRoleNameForCluster;
  private ApiClient apiClient;
  private CoreV1Api coreV1Api;
  private ScheduledExecutorService monitorScheduler;
  private boolean workloadIdentityEnabled;
  private String workloadIdentityPool;
  private String workloadIdentityProvider;
  private RbacAuthorizationV1Api rbacV1Api;

  public KubeTwillRunnerService(MasterEnvironmentContext masterEnvContext,
      ApiClientFactory apiClientFactory,
      String kubeNamespace, DiscoveryServiceClient discoveryServiceClient,
      PodInfo podInfo, String resourcePrefix, Map<String, String> extraLabels,
      boolean enableMonitor,
      boolean workloadIdentityEnabled,
      String workloadLauncherRoleNameForNamespace,
      String workloadLauncherRoleNameForCluster,
      String workloadIdentityPool,
      String workloadIdentityProvider) {
    this.masterEnvContext = masterEnvContext;
    this.apiClientFactory = apiClientFactory;
    this.kubeNamespace = kubeNamespace;
    this.podInfo = podInfo;
    this.resourcePrefix = resourcePrefix;
    this.discoveryServiceClient = discoveryServiceClient;
    this.extraLabels = Collections.unmodifiableMap(new HashMap<>(extraLabels));

    // Selects all runs started by the k8s twill runner that has the run id label
    this.selector = String.format("%s=%s,%s", RUNNER_LABEL, RUNNER_LABEL_VAL, RUN_ID_LABEL);
    // Contains mapping of the Kubernetes namespace to a map of resource types and the watcher threads
    this.resourceWatchers = new HashMap<>();
    this.liveInfos = new ConcurrentSkipListMap<>();
    this.liveInfoLock = new ReentrantLock();
    this.enableMonitor = enableMonitor;
    this.workloadIdentityEnabled = workloadIdentityEnabled;
    this.workloadLauncherRoleNameForNamespace = workloadLauncherRoleNameForNamespace;
    this.workloadLauncherRoleNameForCluster = workloadLauncherRoleNameForCluster;
    this.workloadIdentityPool = workloadIdentityPool;
    this.workloadIdentityProvider = workloadIdentityProvider;
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable) {
    return prepare(runnable, ResourceSpecification.BASIC);
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable,
      ResourceSpecification resourceSpecification) {
    return prepare(new SingleRunnableApplication(runnable, resourceSpecification));
  }

  @Override
  public TwillPreparer prepare(TwillApplication application) {
    TwillSpecification spec = application.configure();
    RunId runId;
    if (application instanceof ExtendedTwillApplication) {
      runId = RunIds.fromString(((ExtendedTwillApplication) application).getRunId());
    } else {
      runId = RunIds.generate();
    }
    Location appLocation = getApplicationLocation(spec.getName(), runId);
    Map<String, String> labels = new HashMap<>(extraLabels);
    labels.put(RUNNER_LABEL, RUNNER_LABEL_VAL);
    labels.put(APP_LABEL, spec.getName());
    labels.put(RUN_ID_LABEL, runId.getId());

    return new KubeTwillPreparer(masterEnvContext, apiClient, kubeNamespace, podInfo,
        spec, runId, appLocation, resourcePrefix, labels,
        (resourceType, meta, timeout, timeoutUnit) -> {
          // Adds the controller to the LiveInfo.
          liveInfoLock.lock();
          try {
            KubeTwillController controller = createKubeTwillController(spec.getName(), runId,
                resourceType, meta);
            if (!enableMonitor) {
              //since monitor is disabled, we fire and forget
              return controller;
            }
            KubeLiveInfo liveInfo = liveInfos.computeIfAbsent(spec.getName(),
                n -> new KubeLiveInfo(resourceType, n));
            return liveInfo.addControllerIfAbsent(runId, timeout, timeoutUnit, controller, meta);
          } finally {
            liveInfoLock.unlock();
          }
        });
  }

  @Nullable
  @Override
  public TwillController lookup(String applicationName, RunId runId) {
    KubeLiveInfo liveInfo = liveInfos.get(applicationName);
    return liveInfo == null ? null : liveInfo.getController(runId);
  }

  @Override
  public Iterable<TwillController> lookup(String applicationName) {
    KubeLiveInfo liveInfo = liveInfos.get(applicationName);
    return liveInfo == null ? Collections.emptyList() : liveInfo.getControllers();
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    // Protect against modifications
    return () -> Collections.unmodifiableCollection(liveInfos.values()).stream()
        .map(LiveInfo.class::cast).iterator();
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(
      @SuppressWarnings("deprecation") SecureStoreUpdater updater,
      long initialDelay, long delay, TimeUnit unit) {
    return () -> {
    };
  }

  @Override
  public Cancellable setSecureStoreRenewer(SecureStoreRenewer renewer, long initialDelay,
      long delay, long retryDelay,
      TimeUnit unit) {
    return () -> {
    };
  }

  @Override
  public void start() {
    LOG.debug("Starting KubeTwillRunnerService with {} monitor",
        enableMonitor ? "enabled" : "disabled");
    try {
      apiClient = apiClientFactory.create();
      coreV1Api = new CoreV1Api(apiClient);
      rbacV1Api = new RbacAuthorizationV1Api(apiClient);
      if (!enableMonitor) {
        return;
      }
      monitorScheduler = Executors.newSingleThreadScheduledExecutor(
          Threads.createDaemonThreadFactory("kube-monitor-executor"));
      addAndStartWatchers(kubeNamespace);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to get Kubernetes API Client", e);
    }
  }

  @Override
  public void onStart(Collection<NamespaceDetail> namespaceDetails) {
    for (NamespaceDetail namespaceDetail : namespaceDetails) {
      String namespace = namespaceDetail.getProperties()
          .get(KubeMasterEnvironment.NAMESPACE_PROPERTY);
      if (namespace != null) {
        addAndStartWatchers(namespace);
      }
    }
  }

  @Override
  public void onNamespaceCreation(NamespaceDetail namespaceDetail) throws Exception {
    String cdapNamespace = namespaceDetail.getName();
    Map<String, String> properties = namespaceDetail.getProperties();
    if (NamespaceId.isReserved(cdapNamespace)) {
      return;
    }

    String namespace = properties.get(KubeMasterEnvironment.NAMESPACE_PROPERTY);
    if (namespace == null || namespace.isEmpty()) {
      throw new IOException(
          String.format("Cannot create Kubernetes namespace for %s because no name was provided",
              cdapNamespace));
    }
    // Kubernetes namespace must be a lowercase RFC 1123 label, consisting of lower case alphanumeric characters or '-'
    // and must start and end with an alphanumeric character
    KubeUtil.validateRFC1123LabelName(namespace);
    findOrCreateKubeNamespace(namespace, cdapNamespace);
    updateOrCreateResourceQuota(namespace, cdapNamespace, properties);
    copySecrets(namespace, cdapNamespace);
    createWorkloadServiceAccount(namespace, cdapNamespace);
    if (workloadIdentityEnabled) {
      String workloadIdentityServiceAccountEmail = properties.get(
          WORKLOAD_IDENTITY_GCP_SERVICE_ACCOUNT_EMAIL_PROPERTY);
      if (workloadIdentityServiceAccountEmail != null
          && !workloadIdentityServiceAccountEmail.isEmpty()) {
        WorkloadIdentityUtil.findOrCreateWorkloadIdentityConfigMap(coreV1Api, namespace,
            workloadIdentityServiceAccountEmail,
            workloadIdentityPool,
            workloadIdentityProvider);
      }
    }
    addAndStartWatchers(namespace);
  }

  @Override
  public void onNamespaceDeletion(NamespaceDetail namespaceDetail) throws Exception {
    String namespace = namespaceDetail.getProperties()
        .get(KubeMasterEnvironment.NAMESPACE_PROPERTY);
    if (namespace != null && !namespace.isEmpty()) {
      deleteKubeNamespace(namespace, namespaceDetail.getName());
      stopAndRemoveWatchers(namespace);
    }
  }

  /**
   * Returns the k8s label selector which selects all runs started by the k8s twill runner that has
   * the run id label.
   */
  public String getSelector() {
    return selector;
  }

  /**
   * Checks if namespace already exists from the same CDAP instance. Otherwise, creates a new
   * Kubernetes namespace.
   */
  private void findOrCreateKubeNamespace(String namespace, String cdapNamespace) throws Exception {
    try {
      V1Namespace existingNamespace = coreV1Api.readNamespace(namespace, null);
      if (existingNamespace.getMetadata() == null) {
        throw new IOException(
            String.format("Kubernetes namespace %s exists but was not created by CDAP", namespace));
      }
      Map<String, String> labels = existingNamespace.getMetadata().getLabels();
      if (labels == null || !cdapNamespace.equals(labels.get(CDAP_NAMESPACE_LABEL))) {
        throw new IOException(
            String.format("Kubernetes namespace %s exists but was not created by CDAP namespace %s",
                namespace, cdapNamespace));
      }
    } catch (ApiException e) {
      if (e.getCode() != HttpURLConnection.HTTP_NOT_FOUND) {
        throw new IOException(
            "Error occurred while checking if Kubernetes namespace already exists. Error code = "
                + e.getCode() + ", Body = " + e.getResponseBody(), e);
      }
      createKubeNamespace(namespace, cdapNamespace);
    }
  }

  private void createKubeNamespace(String namespace, String cdapNamespace) throws Exception {
    V1Namespace namespaceObject = new V1Namespace();
    namespaceObject.setMetadata(
        new V1ObjectMeta().name(namespace).putLabelsItem(CDAP_NAMESPACE_LABEL, cdapNamespace));
    try {
      coreV1Api.createNamespace(namespaceObject, null, null, null, null);
      LOG.debug("Created Kubernetes namespace {} for namespace {}", namespace, cdapNamespace);
    } catch (ApiException e) {
      try {
        deleteKubeNamespace(namespace, cdapNamespace);
      } catch (IOException deletionException) {
        e.addSuppressed(deletionException);
      }
      throw new IOException("Error occurred while creating Kubernetes namespace. Error code = "
          + e.getCode() + ", Body = " + e.getResponseBody(), e);
    }
  }

  /**
   * Updates resource quota if it already exists in the Kubernetes namespace. Otherwise, creates a
   * new resource quota.
   */
  private void updateOrCreateResourceQuota(String namespace, String cdapNamespace,
      Map<String, String> properties)
      throws Exception {

    String kubeCpuLimit = properties.get(NAMESPACE_CPU_LIMIT_PROPERTY);
    String kubeMemoryLimit = properties.get(NAMESPACE_MEMORY_LIMIT_PROPERTY);
    Map<String, Quantity> hardLimitMap = new HashMap<>();
    if (kubeCpuLimit != null && !kubeCpuLimit.isEmpty()) {
      hardLimitMap.put("limits.cpu", new Quantity(kubeCpuLimit));
    }
    if (kubeMemoryLimit != null && !kubeMemoryLimit.isEmpty()) {
      hardLimitMap.put("limits.memory", new Quantity(kubeMemoryLimit));
    }
    if (hardLimitMap.isEmpty()) {
      // no resource limits to create
      return;
    }

    V1ResourceQuota resourceQuota = new V1ResourceQuota();
    resourceQuota.setMetadata(new V1ObjectMeta()
        .name(RESOURCE_QUOTA_NAME)
        .putLabelsItem(CDAP_NAMESPACE_LABEL, cdapNamespace));
    resourceQuota.setSpec(new V1ResourceQuotaSpec().hard(hardLimitMap));
    try {
      V1ResourceQuota existingResourceQuota = coreV1Api.readNamespacedResourceQuota(
          RESOURCE_QUOTA_NAME, namespace,
          null);
      if (existingResourceQuota.getMetadata() == null) {
        throw new IOException(
            String.format("%s exists but was not created by CDAP", RESOURCE_QUOTA_NAME));
      }
      Map<String, String> labels = existingResourceQuota.getMetadata().getLabels();
      if (labels == null || !cdapNamespace.equals(labels.get(CDAP_NAMESPACE_LABEL))) {
        throw new IOException(String.format("%s exists but was not created by CDAP namespace %s",
            RESOURCE_QUOTA_NAME, cdapNamespace));
      }
      if (!hardLimitMap.equals(existingResourceQuota.getSpec().getHard())) {
        coreV1Api.replaceNamespacedResourceQuota(RESOURCE_QUOTA_NAME, namespace, resourceQuota,
            null, null, null, null);
      }
    } catch (ApiException e) {
      if (e.getCode() != HttpURLConnection.HTTP_NOT_FOUND) {
        throw new IOException(
            "Error occurred while checking or updating Kubernetes resource quota. Error code = "
                + e.getCode() + ", Body = " + e.getResponseBody(), e);
      }
      createKubeResourceQuota(namespace, resourceQuota);
    }
  }

  private void createKubeResourceQuota(String namespace, V1ResourceQuota resourceQuota)
      throws Exception {
    try {
      coreV1Api.createNamespacedResourceQuota(namespace, resourceQuota, null, null, null, null);
      LOG.debug("Created resource quota for Kubernetes namespace {}", namespace);
    } catch (ApiException e) {
      throw new IOException("Error occurred while creating Kubernetes resource quota. Error code = "
          + e.getCode() + ", Body = " + e.getResponseBody(), e);
    }
  }

  /**
   * Copy secrets into the new namespace for deployments created via the KubeTwillRunnerService
   * TODO: (CDAP-20087) avoid copying secrets
   */
  private void copySecrets(String namespace, String cdapNamespace) throws IOException {
    try {
      for (V1Volume volume : podInfo.getVolumes()) {
        if (volume.getSecret() != null) {
          String secretName = volume.getSecret().getSecretName();
          V1Secret existingSecret = coreV1Api.readNamespacedSecret(secretName,
              podInfo.getNamespace(),
              null);
          V1Secret secret = new V1Secret().data(existingSecret.getData())
              .type(existingSecret.getType())
              .metadata(new V1ObjectMeta().name(secretName).putLabelsItem(CDAP_NAMESPACE_LABEL,
                  cdapNamespace));
          try {
            coreV1Api.createNamespacedSecret(namespace, secret, null, null, null, null);
          } catch (ApiException e) {
            if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
              throw e;
            }
            LOG.warn("The secret '{}:{}' already exists : {}. Ignoring creation of the secret.",
                namespace,
                secret.getMetadata().getName(), e.getResponseBody());
          }
          LOG.debug("Created secret {} in Kubernetes namespace {}", secretName, namespace);
        }
      }
    } catch (ApiException e) {
      throw new IOException("Error occurred while copying volumes. Error code = "
          + e.getCode() + ", Body = " + e.getResponseBody(), e);
    }
  }

  /**
   * Create service account and role bindings required for workload pod.
   * TODO: (CDAP-18956) improve this logic to be for each pipeline run
   */
  private void createWorkloadServiceAccount(String namespace, String cdapNamespace)
      throws IOException {
    try {
      // Create service account for workload pod
      // TODO(CDAP-19149): Cleanup strong coupling currently present in CDAP service accounts to avoid copying.
      String serviceAccountName = podInfo.getServiceAccountName();
      createServiceAccount(namespace, cdapNamespace, serviceAccountName);

      // Create namespace-specific role-binding for the workload service account
      createNamespacedRoleBinding(WORKLOAD_LAUNCHER_NAMESPACE_ROLE_BINDING_NAME, CLUSTER_ROLE_KIND,
          workloadLauncherRoleNameForNamespace, namespace, serviceAccountName, cdapNamespace);

      // Create cluster-wide role-binding for the workload service account
      String workloadLauncherClusterRoleBindingName = String.format(
          WORKLOAD_LAUNCHER_CLUSTER_ROLE_BINDING_FORMAT,
          namespace);
      createClusterRoleBinding(workloadLauncherClusterRoleBindingName,
          workloadLauncherRoleNameForCluster, namespace,
          serviceAccountName, cdapNamespace);

    } catch (ApiException e) {
      throw new IOException(
          "Error occurred while creating service account or role binding. Error code = "
              + e.getCode() + ", Body = " + e.getResponseBody(), e);
    }
  }

  private void createServiceAccount(String namespace, String cdapNamespace,
      String serviceAccountName)
      throws ApiException {
    V1ServiceAccount serviceAccount = new V1ServiceAccount()
        .metadata(new V1ObjectMeta().name(serviceAccountName)
            .putLabelsItem(CDAP_NAMESPACE_LABEL, cdapNamespace));
    try {
      coreV1Api.createNamespacedServiceAccount(namespace, serviceAccount, null, null, null, null);
    } catch (ApiException e) {
      if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
        throw e;
      }
      LOG.warn(
          "The service account '{}:{}' already exists : {}. Ignoring creation of the service account.",
          namespace,
          serviceAccountName, e.getResponseBody());
    }
    LOG.info("Created serviceAccount {} in Kubernetes namespace {}", serviceAccountName, namespace);
  }

  private void createNamespacedRoleBinding(String bindingName, String roleKind, String roleName,
      String namespace,
      String serviceAccountName, String cdapNamespace) throws ApiException {
    KubeUtil.validatePathSegmentName(bindingName);
    V1RoleBinding namespaceWorkloadLauncherBinding = new V1RoleBindingBuilder()
        .withMetadata(new V1ObjectMetaBuilder()
            .withNamespace(namespace)
            .withName(bindingName)
            .build().putLabelsItem(CDAP_NAMESPACE_LABEL, cdapNamespace))
        .withRoleRef(new V1RoleRefBuilder()
            .withApiGroup(RBAC_V1_API_GROUP)
            .withKind(roleKind)
            .withName(roleName).build())
        .withSubjects(new V1SubjectBuilder()
            .withKind(SERVICE_ACCOUNT_KIND)
            .withName(serviceAccountName).build())
        .build();
    try {
      rbacV1Api.createNamespacedRoleBinding(namespace, namespaceWorkloadLauncherBinding, null, null,
          null, null);
    } catch (ApiException e) {
      if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
        throw e;
      }
      LOG.warn(
          "The role binding '{}:{}' already exists : {}. Ignoring creation of the role binding.",
          namespace,
          bindingName, e.getResponseBody());
    }

    LOG.info("Created namespace role binding '{}' in k8s namespace '{}' for service account '{}'",
        bindingName, serviceAccountName, serviceAccountName);
  }

  private void createClusterRoleBinding(String bindingName, String roleName,
      String serviceAccountNamespace,
      String serviceAccountName, String cdapNamespace) throws ApiException {
    KubeUtil.validatePathSegmentName(bindingName);
    V1ClusterRoleBinding clusterWorkloadLauncherBinding = new V1ClusterRoleBindingBuilder()
        .withMetadata(new V1ObjectMetaBuilder()
            .withName(bindingName).build().putLabelsItem(
                CDAP_NAMESPACE_LABEL, cdapNamespace))
        .withRoleRef(new V1RoleRefBuilder()
            .withApiGroup(RBAC_V1_API_GROUP)
            .withKind(CLUSTER_ROLE_KIND)
            .withName(roleName).build())
        .withSubjects(new V1SubjectBuilder()
            .withKind(SERVICE_ACCOUNT_KIND)
            .withNamespace(serviceAccountNamespace)
            .withName(serviceAccountName).build())
        .build();
    try {
      rbacV1Api.createClusterRoleBinding(clusterWorkloadLauncherBinding, null, null, null, null);
    } catch (ApiException e) {
      if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
        throw e;
      }
      LOG.warn(
          "The cluster role binding '{}' already exists : {}. Ignoring creation of the cluster role binding.",
          bindingName, e.getResponseBody());
    }

    LOG.info("Created cluster role binding '{}' for service account '{}' in k8s namespace '{}'",
        bindingName,
        serviceAccountName, serviceAccountNamespace);
  }

  /**
   * Deletes Kubernetes namespace created by CDAP and associated resources if they exist.
   */
  private void deleteKubeNamespace(String namespace, String cdapNamespace) throws Exception {
    try {
      V1Namespace namespaceObject = coreV1Api.readNamespace(namespace, null);
      if (namespaceObject.getMetadata() != null) {
        Map<String, String> namespaceLabels = namespaceObject.getMetadata().getLabels();
        if (namespaceLabels != null && namespaceLabels.get(CDAP_NAMESPACE_LABEL)
            .equals(cdapNamespace)) {
          // PropagationPolicy is set to background cascading deletion. Kubernetes deletes the owner object immediately
          // and the controller cleans up the dependent objects in the background.
          coreV1Api.deleteNamespace(namespace, null, null, 0, null, "Background", null);
          LOG.info("Deleted Kubernetes namespace and associated resources for {}", namespace);
          return;
        }
      }
      LOG.debug(
          "Kubernetes namespace {} was not deleted because it was not created by CDAP namespace {}",
          namespace, cdapNamespace);
    } catch (ApiException e) {
      if (e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        LOG.debug("Kubernetes namespace {} was not deleted because it was not found", namespace);
      } else {
        throw new IOException("Error occurred while deleting Kubernetes namespace. Error code = "
            + e.getCode() + ", Body = " + e.getResponseBody(), e);
      }
    }
  }

  @Override
  public void stop() {
    stopAndRemoveWatchers();
    if (monitorScheduler != null) {
      monitorScheduler.shutdownNow();
    }
  }

  @VisibleForTesting
  void setCoreV1Api(CoreV1Api coreV1Api) {
    this.coreV1Api = coreV1Api;
  }

  @VisibleForTesting
  void setRbacV1Api(RbacAuthorizationV1Api rbacV1Api) {
    this.rbacV1Api = rbacV1Api;
  }

  @VisibleForTesting
  void setWorkloadIdentityEnabled() {
    this.workloadIdentityEnabled = true;
  }

  @VisibleForTesting
  void setWorkloadIdentityPool(String workloadIdentityPool) {
    this.workloadIdentityPool = workloadIdentityPool;
  }

  @VisibleForTesting
  void setWorkloadIdentityProvider(String workloadIdentityProvider) {
    this.workloadIdentityProvider = workloadIdentityProvider;
  }

  /**
   * Monitors the given {@link KubeTwillController}.
   *
   * @param liveInfo the {@link KubeLiveInfo} that the controller belongs to
   * @param timeout the start timeout
   * @param timeoutUnit the start timeout unit
   * @param controller the controller top monitor
   * @param <T> the type of the resource to watch
   * @param resourceType resource type being controlled by controller
   * @param startupTaskCompletion startup task completion
   * @return the controller
   */
  private <T extends KubernetesObject> KubeTwillController monitorController(
      KubeLiveInfo liveInfo, long timeout, TimeUnit timeoutUnit, KubeTwillController controller,
      AppResourceWatcherThread<T> watcher, Type resourceType,
      CompletableFuture<Void> startupTaskCompletion) {

    String runId = controller.getRunId().getId();
    if (!enableMonitor) {
      throw new UnsupportedOperationException(
          String.format("Cannot monitor controller for run %s when monitoring is disabled", runId));
    }

    LOG.debug("Monitoring application {} with run {} starts in {} {}",
        liveInfo.getApplicationName(), runId, timeout, timeoutUnit);

    // Schedule to terminate the controller in the timeout time.
    Future<?> terminationFuture = monitorScheduler.schedule(controller::terminateOnTimeout, timeout,
        timeoutUnit);

    // This future is for transferring the cancel watch to the change listener
    CompletableFuture<Cancellable> cancellableFuture = new CompletableFuture<>();

    // Listen to resource changes. If the resource represented by the controller has all replicas ready, cancel
    // the terminationFuture. If the resource is deleted, also cancel the terminationFuture, and also terminate
    // the controller as we no longer need to watch for any future changes.
    Cancellable cancellable = watcher.addListener(new ResourceChangeListener<T>() {
      @Override
      public void resourceAdded(T resource) {
        // Handle the same way as modified
        resourceModified(resource);
      }

      @Override
      public void resourceModified(T resource) {
        V1ObjectMeta metadata = resource.getMetadata();

        if (!runId.equals(metadata.getLabels().get(RUN_ID_LABEL))) {
          return;
        }

        if (resourceType.equals(V1Job.class)) {
          // If job has status active we consider it as ready
          if (isJobReady((V1Job) resource)) {
            LOG.debug("Application {} with run {} is available in Kubernetes",
                liveInfo.getApplicationName(), runId);
            startupTaskCompletion.complete(null);
            // Cancel the scheduled termination
            terminationFuture.cancel(false);
          }

          // If job is in terminal state - success/failure - we consider it as complete.
          if (isJobComplete((V1Job) resource)) {
            // Cancel the watch
            try {
              Uninterruptibles.getUninterruptibly(cancellableFuture).cancel();
            } catch (ExecutionException e) {
              // This will never happen
            }

            controller.setJobStatus(((V1Job) resource).getStatus());
            // terminate the job controller
            controller.terminate();
          }
        } else {
          if (isAllReplicasReady(resource)) {
            LOG.debug("Application {} with run {} is available in Kubernetes",
                liveInfo.getApplicationName(), runId);
            // Cancel the scheduled termination
            terminationFuture.cancel(false);
            // Cancel the watch
            try {
              Uninterruptibles.getUninterruptibly(cancellableFuture).cancel();
            } catch (ExecutionException e) {
              // This will never happen
            }
          }
        }
      }

      @Override
      public void resourceDeleted(T resource) {
        V1ObjectMeta metadata = resource.getMetadata();

        // If the run is deleted, terminate the controller right away and cancel the scheduled termination
        if (runId.equals(metadata.getLabels().get(RUN_ID_LABEL))) {
          // Cancel the scheduled termination
          terminationFuture.cancel(false);
          // Cancel the watch
          try {
            Uninterruptibles.getUninterruptibly(cancellableFuture).cancel();
          } catch (ExecutionException e) {
            // This will never happen
          }
          controller.terminate();
        }
      }
    });

    cancellableFuture.complete(cancellable);

    // On controller termination, remove it from the liveInfo
    controller.onTerminated(() -> {
      // Cancel the scheduled termination
      terminationFuture.cancel(false);
      // Cancel the watch if there is one
      cancellable.cancel();
      liveInfoLock.lock();
      try {
        liveInfo.removeController(controller);
        if (liveInfo.isEmpty()) {
          liveInfos.remove(liveInfo.getApplicationName(), liveInfo);
        }
      } finally {
        liveInfoLock.unlock();
      }

      if (!resourceType.equals(V1Job.class)) {
        try {
          Uninterruptibles.getUninterruptibly(controller.terminate());
          LOG.debug("Controller for application {} of run {} is terminated",
              liveInfo.getApplicationName(), runId);
        } catch (ExecutionException e) {
          LOG.error("Controller for application {} of run {} is terminated due to failure",
              liveInfo.getApplicationName(), runId, e.getCause());
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    return controller;
  }

  /**
   * Returns the application {@link Location} for staging files.
   *
   * @param name name of the twill application
   * @param runId the runId
   * @return the application {@link Location}
   */
  private Location getApplicationLocation(String name, RunId runId) {
    return masterEnvContext.getLocationFactory()
        .create(String.format("twill/%s/%s", name, runId.getId()));
  }

  /**
   * Checks if number of requested replicas is the same as the number of ready replicas in the given
   * resource.
   */
  private boolean isAllReplicasReady(Object resource) {
    try {
      Method getStatus = resource.getClass().getDeclaredMethod("getStatus");
      Object statusObj = getStatus.invoke(resource);

      Method getReplicas = statusObj.getClass().getDeclaredMethod("getReplicas");
      Integer replicas = (Integer) getReplicas.invoke(statusObj);

      Method getReadyReplicas = statusObj.getClass().getDeclaredMethod("getReadyReplicas");
      Integer readyReplicas = (Integer) getReadyReplicas.invoke(statusObj);

      return replicas != null && Objects.equals(replicas, readyReplicas);

    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | ClassCastException e) {
      LOG.warn("Failed to get number of replicas and ready replicas from the resource {}", resource,
          e);
      return false;
    }
  }

  /**
   * Checks if job is ready.
   */
  private boolean isJobReady(V1Job job) {
    V1JobStatus jobStatus = job.getStatus();
    if (jobStatus != null) {
      Integer active = jobStatus.getActive();
      if (active == null) {
        return false;
      }

      try {
        String labelSelector = job.getMetadata().getLabels().entrySet().stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining(","));

        // Make sure at least one pod launched from the job is in active state.
        // https://github.com/kubernetes-client/java/blob/master/kubernetes/docs/V1JobStatus.md
        V1PodList podList = coreV1Api.listNamespacedPod(job.getMetadata().getNamespace(), null,
            null, null, null,
            labelSelector, null, null, null, null, null);

        for (V1Pod pod : podList.getItems()) {
          if (pod.getStatus() != null && pod.getStatus().getPhase() != null
              && pod.getStatus().getPhase().equalsIgnoreCase("RUNNING")) {
            return true;
          }
        }
      } catch (ApiException e) {
        // If there is an exception while getting active pods for a job, we will use job level status.
        LOG.warn("Error while getting active pods for job {}, {}", job.getMetadata().getName(),
            e.getResponseBody());
      }
    }
    return false;
  }

  /**
   * Checks if job is complete. Job completion can be in success or failed state.
   */
  private boolean isJobComplete(V1Job job) {
    V1JobStatus jobStatus = job.getStatus();
    if (jobStatus != null) {
      return jobStatus.getFailed() != null || jobStatus.getSucceeded() != null;
    }
    return false;
  }

  /**
   * An {@link ResourceChangeListener} to watch for changes in application resources. It is for
   * refreshing the liveInfos map.
   */
  private final class AppResourceChangeListener<T extends KubernetesObject> implements
      ResourceChangeListener<T> {

    @Override
    public void resourceAdded(T resource) {
      V1ObjectMeta metadata = resource.getMetadata();
      String appName = metadata.getAnnotations().get(APP_LABEL);
      if (appName == null) {
        // This shouldn't happen. Just to guard against future bug.
        return;
      }
      String resourceName = metadata.getName();
      RunId runId = RunIds.fromString(metadata.getLabels().get(RUN_ID_LABEL));

      // Read the start timeout millis from the annotation. It is set by the KubeTwillPreparer
      long startTimeoutMillis = TimeUnit.SECONDS.toMillis(120);
      try {
        startTimeoutMillis = Long.parseLong(
            metadata.getAnnotations().get(START_TIMEOUT_ANNOTATION));
      } catch (Exception e) {
        // This shouldn't happen
        LOG.warn(
            "Failed to get start timeout from the annotation using key {} from resource {}. Defaulting to {} ms",
            START_TIMEOUT_ANNOTATION, metadata.getName(), startTimeoutMillis, e);
      }

      // Add the LiveInfo and Controller
      liveInfoLock.lock();
      try {
        KubeLiveInfo liveInfo = liveInfos.computeIfAbsent(appName,
            k -> new KubeLiveInfo(resource.getClass(), appName));
        KubeTwillController controller = createKubeTwillController(appName, runId,
            resource.getClass(), metadata);
        liveInfo.addControllerIfAbsent(runId, startTimeoutMillis, TimeUnit.MILLISECONDS, controller,
            metadata);
      } finally {
        liveInfoLock.unlock();
      }
    }

    @Override
    public void resourceDeleted(T resource) {
      V1ObjectMeta metadata = resource.getMetadata();
      String appName = metadata.getAnnotations().get(APP_LABEL);
      if (appName == null) {
        // This shouldn't happen. Just to guard against future bug.
        return;
      }

      // Get and terminate the controller
      liveInfoLock.lock();
      try {
        KubeLiveInfo liveInfo = liveInfos.get(appName);
        if (liveInfo != null) {
          RunId runId = RunIds.fromString(metadata.getLabels().get(RUN_ID_LABEL));
          if (!liveInfo.resourceType.equals(V1Job.class)) {
            Optional.ofNullable(liveInfo.getController(runId))
                .ifPresent(KubeTwillController::terminate);
          }
        }
      } finally {
        liveInfoLock.unlock();
      }
    }
  }

  /**
   * Creates a {@link KubeTwillController}.
   */
  private KubeTwillController createKubeTwillController(String appName, RunId runId,
      Type resourceType, V1ObjectMeta meta) {
    CompletableFuture<Void> startupTaskCompletion = new CompletableFuture<>();
    KubeTwillController controller = new KubeTwillController(meta.getNamespace(), runId,
        discoveryServiceClient,
        apiClient, resourceType, meta, startupTaskCompletion);

    Location appLocation = getApplicationLocation(appName, runId);
    controller.onTerminated(() -> {
      try {
        appLocation.delete(true);
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to delete location for " + appName + "-" + runId + " at " + appLocation, e);
      }
    }, command -> new Thread(command, "app-cleanup-" + appName).start());
    return controller;
  }

  /**
   * A TwillApplication with a single runnable.
   */
  private static class SingleRunnableApplication implements TwillApplication {

    private final TwillRunnable runnable;
    private final ResourceSpecification resourceSpec;

    SingleRunnableApplication(TwillRunnable runnable, ResourceSpecification resourceSpec) {
      this.runnable = runnable;
      this.resourceSpec = resourceSpec;
    }

    @Override
    public TwillSpecification configure() {
      TwillRunnableSpecification runnableSpec = runnable.configure();
      return TwillSpecification.Builder.with()
          .setName(runnableSpec.getName())
          .withRunnable().add(runnableSpec.getName(), runnable, resourceSpec)
          .noLocalFiles()
          .anyOrder()
          .build();
    }
  }

  /**
   * Create and start watchers for the given Kubernetes namespace
   */
  private synchronized void addAndStartWatchers(String namespace) {
    if (resourceWatchers.containsKey(namespace)) {
      return;
    }
    Map<Type, AppResourceWatcherThread<?>> typeMap = new HashMap<>();
    // Batch jobs are k8s jobs, and streaming pipelines are k8s deployments
    typeMap.put(V1Job.class,
        AppResourceWatcherThread.createJobWatcher(namespace, selector, apiClientFactory));
    // We only create deployments and statefulsets in the system namespace,
    // so only add watchers for them in that namespace
    if (namespace.equals(kubeNamespace)) {
      typeMap.put(V1Deployment.class,
          AppResourceWatcherThread.createDeploymentWatcher(namespace, selector, apiClientFactory));
      typeMap.put(V1StatefulSet.class,
          AppResourceWatcherThread.createStatefulSetWatcher(namespace, selector, apiClientFactory));
    }
    typeMap.values().forEach(watcher -> {
      watcher.addListener(new AppResourceChangeListener<>());
      watcher.start();
    });
    resourceWatchers.put(namespace, typeMap);
  }

  /**
   * Stop and remove watchers for the given Kubernetes namespace
   */
  private synchronized void stopAndRemoveWatchers(String namespace) {
    if (!resourceWatchers.containsKey(namespace)) {
      LOG.warn("Job watcher does not exist for namespace {}", namespace);
      return;
    }
    resourceWatchers.get(namespace).values().forEach(AbstractWatcherThread::close);
    resourceWatchers.remove(namespace);
  }

  /**
   * Stop and remove watchers for all Kubernetes namespaces
   */
  private synchronized void stopAndRemoveWatchers() {
    resourceWatchers.keySet().forEach(this::stopAndRemoveWatchers);
  }

  /**
   * Kubernetes LiveInfo.
   */
  private final class KubeLiveInfo implements LiveInfo {

    private final Type resourceType;
    private final String applicationName;
    private final Map<String, KubeTwillController> controllers;

    KubeLiveInfo(Type resourceType, String applicationName) {
      this.resourceType = resourceType;
      this.applicationName = applicationName;
      this.controllers = new ConcurrentSkipListMap<>();
    }

    KubeTwillController addControllerIfAbsent(RunId runId, long timeout, TimeUnit timeoutUnit,
        KubeTwillController controller, V1ObjectMeta meta) {
      KubeTwillController existing = controllers.putIfAbsent(runId.getId(), controller);
      if (existing != null) {
        return existing;
      }
      String namespace = meta.getNamespace();
      // If it is newly added controller, monitor it.
      addAndStartWatchers(namespace);
      return monitorController(this, timeout, timeoutUnit, controller,
          resourceWatchers.get(namespace).get(resourceType),
          resourceType, controller.getStartedFuture());
    }

    /**
     * Remove the given controller instance.
     *
     * @param controller the instance to remove
     */
    void removeController(KubeTwillController controller) {
      controllers.remove(controller.getRunId().getId(), controller);
    }

    boolean isEmpty() {
      return controllers.isEmpty();
    }

    @Override
    public String getApplicationName() {
      return applicationName;
    }

    @Override
    public Iterable<TwillController> getControllers() {
      // Protect against modifications
      return () -> Collections.unmodifiableCollection(controllers.values())
          .stream().map(TwillController.class::cast).iterator();
    }

    /**
     * Returns the {@link KubeTwillController} for the given {@link RunId}.
     */
    @Nullable
    KubeTwillController getController(RunId runId) {
      return controllers.get(runId.getId());
    }
  }

}
