/*
 * Copyright © 2019-2021 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import io.cdap.cdap.k8s.common.AbstractWatcherThread;
import io.cdap.cdap.k8s.common.DefaultLocalFileProvider;
import io.cdap.cdap.k8s.common.LocalFileProvider;
import io.cdap.cdap.k8s.discovery.KubeDiscoveryService;
import io.cdap.cdap.k8s.identity.GCPWorkloadIdentityCredential;
import io.cdap.cdap.k8s.runtime.KubeTwillRunnerService;
import io.cdap.cdap.k8s.util.KubeUtil;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnable;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnableContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentTask;
import io.cdap.cdap.master.spi.environment.spark.SparkConfig;
import io.cdap.cdap.master.spi.environment.spark.SparkDriverWatcher;
import io.cdap.cdap.master.spi.environment.spark.SparkLocalizeResource;
import io.cdap.cdap.master.spi.environment.spark.SparkSubmitContext;
import io.cdap.cdap.proto.id.NamespaceId;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.V1ClusterRoleBinding;
import io.kubernetes.client.openapi.models.V1ClusterRoleBindingBuilder;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapBuilder;
import io.kubernetes.client.openapi.models.V1ConfigMapProjection;
import io.kubernetes.client.openapi.models.V1ConfigMapVolumeSourceBuilder;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerBuilder;
import io.kubernetes.client.openapi.models.V1DownwardAPIVolumeFile;
import io.kubernetes.client.openapi.models.V1DownwardAPIVolumeSource;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1KeyToPath;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1OwnerReferenceBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodSpecBuilder;
import io.kubernetes.client.openapi.models.V1ProjectedVolumeSource;
import io.kubernetes.client.openapi.models.V1ResourceQuota;
import io.kubernetes.client.openapi.models.V1ResourceQuotaSpec;
import io.kubernetes.client.openapi.models.V1RoleBinding;
import io.kubernetes.client.openapi.models.V1RoleBindingBuilder;
import io.kubernetes.client.openapi.models.V1RoleRefBuilder;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1SecretVolumeSource;
import io.kubernetes.client.openapi.models.V1ServiceAccount;
import io.kubernetes.client.openapi.models.V1ServiceAccountTokenProjection;
import io.kubernetes.client.openapi.models.V1SubjectBuilder;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.openapi.models.V1VolumeProjection;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Yaml;
import io.kubernetes.client.util.generic.options.ListOptions;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

/**
 * Implementation of {@link MasterEnvironment} to provide the environment for running in Kubernetes.
 */
public class KubeMasterEnvironment implements MasterEnvironment {
  public static final String DISABLE_POD_DELETION = "disablePodDeletion";
  public static final String NAMESPACE_PROPERTY = "k8s.namespace";
  public static final String NAMESPACE_CPU_LIMIT_PROPERTY = "k8s.namespace.cpu.limits";
  public static final String NAMESPACE_MEMORY_LIMIT_PROPERTY = "k8s.namespace.memory.limits";
  private static final Logger LOG = LoggerFactory.getLogger(KubeMasterEnvironment.class);

  public static final String SECURITY_CONFIG_NAME = "cdap-security";
  public static final String RESOURCE_QUOTA_NAME = "cdap-resource-quota";
  // Contains the list of configuration / secret names coming from the Pod information, which are
  // needed to propagate to deployments created via the KubeTwillRunnerService
  private static final Set<String> CONFIG_NAMES = ImmutableSet.of("cdap-conf", "hadoop-conf", "cdap-security");
  private static final Set<String> CUSTOM_VOLUME_PREFIX = ImmutableSet.of("cdap-cm-vol-", "cdap-se-vol-");

  private static final String MASTER_MAX_INSTANCES = "master.service.max.instances";
  private static final String DATA_TX_ENABLED = "data.tx.enabled";
  private static final String JOB_CLEANUP_INTERVAL = "program.container.cleaner.interval.mins";
  private static final String JOB_CLEANUP_BATCH_SIZE = "program.container.cleaner.batch.size";

  private static final String NAMESPACE_KEY = "master.environment.k8s.namespace";
  private static final String INSTANCE_LABEL = "master.environment.k8s.instance.label";
  // Label for the container name
  private static final String CONTAINER_LABEL = "master.environment.k8s.container.label";
  private static final String POD_INFO_DIR = "master.environment.k8s.pod.info.dir";
  private static final String POD_NAME_FILE = "master.environment.k8s.pod.name.file";
  private static final String POD_UID_FILE = "master.environment.k8s.pod.uid.file";
  private static final String POD_LABELS_FILE = "master.environment.k8s.pod.labels.file";
  private static final String POD_NAMESPACE_FILE = "master.environment.k8s.pod.namespace.file";
  private static final String POD_KILLER_SELECTOR = "master.environment.k8s.pod.killer.selector";
  private static final String POD_KILLER_DELAY_MILLIS = "master.environment.k8s.pod.killer.delay.millis";
  private static final String SPARK_CONFIGS_PREFIX = "spark.kubernetes";
  private static final String SPARK_KUBERNETES_DRIVER_LABEL_PREFIX = "spark.kubernetes.driver.label.";
  private static final String SPARK_KUBERNETES_EXECUTOR_LABEL_PREFIX = "spark.kubernetes.executor.label.";
  private static final String SPARK_KUBERNETES_NAMESPACE = "spark.kubernetes.namespace";
  @VisibleForTesting
  static final String SPARK_KUBERNETES_DRIVER_POD_TEMPLATE = "spark.kubernetes.driver.podTemplateFile";
  @VisibleForTesting
  static final String SPARK_KUBERNETES_EXECUTOR_POD_TEMPLATE = "spark.kubernetes.executor.podTemplateFile";
  private static final String SPARK_KUBERNETES_DRIVER_SERVICE_ACCOUNT
    = "spark.kubernetes.authenticate.driver.serviceAccountName";
  private static final String SPARK_KUBERNETES_EXECUTOR_SERVICE_ACCOUNT
    = "spark.kubernetes.authenticate.executor.serviceAccountName";
  private static final String SPARK_KUBERNETES_METRICS_PROPERTIES_CONF = "spark.metrics.conf";
  private static final String POD_TEMPLATE_FILE_NAME = "podTemplate-";
  private static final String CDAP_LOCALIZE_FILES_PATH = "/etc/cdap/localizefiles";
  private static final String CDAP_CONFIG_MAP_PREFIX = "cdap-compressed-files-";
  private static final String CDAP_NAMESPACE_LABEL = "cdap.namespace";
  private static final String SPARK_DRIVER_POD_CPU_REQUEST = "spark.kubernetes.driver.request.cores";
  private static final String SPARK_DRIVER_POD_CPU_LIMIT = "spark.kubernetes.driver.limit.cores";
  private static final String SPARK_EXECUTOR_POD_CPU_REQUEST = "spark.kubernetes.executor.request.cores";
  private static final String SPARK_EXECUTOR_POD_CPU_LIMIT = "spark.kubernetes.executor.limit.cores";

  private static final String PROGRAM_CPU_MULTIPLIER = "program.k8s.container.cpu.multiplier";
  private static final String DEFAULT_PROGRAM_CPU_MULTIPLIER = "0.5";

  // Workload Identity Constants
  private static final String WORKLOAD_IDENTITY_ENABLED = "master.environment.k8s.workload.identity.enabled";
  private static final String WORKLOAD_IDENTITY_POOL = "master.environment.k8s.workload.identity.pool";
  @VisibleForTesting
  static final String WORKLOAD_IDENTITY_GCP_SERVICE_ACCOUNT_EMAIL_PROPERTY =
    "workload.identity.gcp.service.account.email";
  private static final String WORKLOAD_IDENTITY_PROVIDER = "master.environment.k8s.workload.identity.provider";
  private static final String WORKLOAD_IDENTITY_SERVICE_ACCOUNT_TOKEN_TTL_SECONDS
    = "master.environment.k8s.workload.identity.service.account.token.ttl.seconds";
  private static final long WORKLOAD_IDENTITY_SERVICE_ACCOUNT_TOKEN_TTL_SECONDS_DEFAULT = 172800L;
  private static final String WORKLOAD_IDENTITY_CONFIGMAP_NAME = "workload-identity-config";
  private static final String WORKLOAD_IDENTITY_PROJECTED_VOLUME_NAME = "gcp-ksa";
  private static final String WORKLOAD_IDENTITY_CONFIGMAP_KEY = "config";
  private static final String WORKLOAD_IDENTITY_CONFIGMAP_FILE = "google-application-credentials.json";
  private static final String WORKLOAD_IDENTITY_DATA_KEY = "config";
  private static final String WORKLOAD_IDENTITY_AUDIENCE_FORMAT = "identitynamespace:%s:%s";
  private static final String WORKLOAD_IDENTITY_IMPERSONATION_URL_FORMAT = "https://iamcredentials.googleapis.com/" +
    "v1/projects/-/serviceAccounts/%s:generateAccessToken";
  private static final String WORKLOAD_IDENTITY_TOKEN_URL = "https://sts.googleapis.com/v1/token";
  private static final String WORKLOAD_IDENTITY_CREDENTIAL_DIR = "/var/run/secrets/tokens/gcp-ksa";
  private static final String WORKLOAD_IDENTITY_CREDENTIAL_KSA_PATH = "token";
  private static final String WORKLOAD_IDENTITY_CREDENTIAL_KSA_SOURCE_PATH = WORKLOAD_IDENTITY_CREDENTIAL_DIR + "/" +
    WORKLOAD_IDENTITY_CREDENTIAL_KSA_PATH;
  private static final String WORKLOAD_IDENTITY_CREDENTIAL_GSA_SOURCE_PATH = WORKLOAD_IDENTITY_CREDENTIAL_DIR + "/" +
    WORKLOAD_IDENTITY_CONFIGMAP_FILE;

  // Workload Launcher Constants
  /**
   * The ClusterRole which defines the permissions required by the workload pod for the namespace in which it is
   * running, e.g. pod, deployment, and statefulset creation permissions. The CDAP system service account must be
   * assigned this role at the cluster level.
   */
  private static final String WORKLOAD_LAUNCHER_NAMESPACE_ROLE_NAME
    = "master.environment.k8s.workload.launcher.namespace.role.name";
  /**
   * The ClusterRole which defines the permissions required by the workload pod across all namespaces, e.g. service get
   * and list permissions for DiscoveryService. The CDAP system service account must be assigned this role at the
   * cluster level.
   */
  private static final String WORKLOAD_LAUNCHER_CLUSTER_ROLE_NAME
    = "master.environment.k8s.workload.launcher.cluster.role.name";
  private static final String DEFAULT_WORKLOAD_LAUNCHER_NAMESPACE_ROLE_NAME = "cdap-workload-launcher";
  private static final String DEFAULT_WORKLOAD_LAUNCHER_CLUSTER_ROLE_NAME = "cdap-cluster-workload-launcher";
  private static final String WORKLOAD_LAUNCHER_NAMESPACE_ROLE_BINDING_NAME
    = "cdap-workload-launcher-namespace-role-binding";
  private static final String WORKLOAD_LAUNCHER_CLUSTER_ROLE_BINDING_FORMAT
    = "cdap-workload-launcher-cluster-role-binding-%s";
  private static final String RBAC_V1_API_GROUP = "rbac.authorization.k8s.io";
  private static final String CLUSTER_ROLE_KIND = "ClusterRole";
  private static final String SERVICE_ACCOUNT_KIND = "ServiceAccount";

  private static final String DEFAULT_NAMESPACE = "default";
  private static final String DEFAULT_INSTANCE_LABEL = "cdap.instance";
  private static final String DEFAULT_CONTAINER_LABEL = "cdap.container";
  private static final String DEFAULT_POD_INFO_DIR = "/etc/podinfo";
  private static final String DEFAULT_POD_NAME_FILE = "pod.name";
  private static final String DEFAULT_POD_UID_FILE = "pod.uid";
  private static final String DEFAULT_POD_NAMESPACE_FILE = "pod.namespace";
  private static final String DEFAULT_POD_LABELS_FILE = "pod.labels.properties";
  private static final long DEFAULT_POD_KILLER_DELAY_MILLIS = TimeUnit.HOURS.toMillis(1L);

  // Environment Property Names used by JMX metrics collector to tag metrics.
  // Names better be short to reduce the serialized metric value size.
  // Common prefix is added to properties to help main services identify tags relevant to
  // environment properties.
  // Name of the CDAP service running in this pod. Eg: appfabric, messaging, metrics etc.
  private static final String COMPONENT_ENV_PROPERTY =
    MasterEnvironmentContext.ENVIRONMENT_PROPERTY_PREFIX + "cmp";
  // Name of the Kubernetes namespace this pod is deployed in. Eg: default
  private static final String NAMESPACE_ENV_PROPERTY =
    MasterEnvironmentContext.ENVIRONMENT_PROPERTY_PREFIX + "ns";

  private static final Pattern LABEL_PATTERN = Pattern.compile("(cdap\\..+?)=\"(.*)\"");
  private static final Pattern NAMESPACE_LABEL_PATTERN = Pattern.compile("(k8s\\.namespace)=\"(.*)\"");

  private static final String SPARK_KUBERNETES_DRIVER_CONTAINER_VALUE = "spark-kubernetes-driver";
  private static final String SPARK_KUBERNETES_EXECUTOR_CONTAINER_VALUE = "spark-kubernetes-executor";
  private static final String SPARK_ROLE_LABEL = "spark-role";
  private static final String SPARK_DRIVER_LABEL_VALUE = "driver";
  private static final String CDAP_CONTAINER_LABEL = "cdap.container";
  private static final String TWILL_RUNNER_SERVICE_MONITOR_DISABLE = "twill.runner.service.monitor.disable";

  private KubeDiscoveryService discoveryService;
  private PodKillerTask podKillerTask;
  private KubeTwillRunnerService twillRunner;
  private PodInfo podInfo;
  private Map<String, String> additionalSparkConfs;
  private File podInfoDir;
  private File podLabelsFile;
  private File podNameFile;
  private File podUidFile;
  private File podNamespaceFile;
  // In memory state for holding configmap name. Used to delete this configmap upon master environment destroy.
  private String configMapName;
  private CoreV1Api coreV1Api;
  private RbacAuthorizationV1Api rbacV1Api;
  private KubeMasterPathProvider kubeMasterPathProvider;
  private LocalFileProvider localFileProvider;
  private final Gson gson;
  private boolean workloadIdentityEnabled;
  private String workloadIdentityPool;
  private String workloadIdentityProvider;
  private long workloadIdentityServiceAccountTokenTTLSeconds;
  private String workloadLauncherRoleNameForNamespace;
  private String workloadLauncherRoleNameForCluster;
  private String programCpuMultiplier;
  private List<V1SecretVolumeSource> secretVolumes;

  public KubeMasterEnvironment() {
    gson = new Gson();
  }

  @Override
  public void initialize(MasterEnvironmentContext context) throws IOException, IllegalArgumentException, ApiException {
    LOG.info("Initializing Kubernetes environment");

    Map<String, String> conf = context.getConfigurations();
    kubeMasterPathProvider = new DefaultKubeMasterPathProvider();
    localFileProvider = new DefaultLocalFileProvider();
    podInfoDir = new File(conf.getOrDefault(POD_INFO_DIR, DEFAULT_POD_INFO_DIR));
    podLabelsFile = new File(podInfoDir, conf.getOrDefault(POD_LABELS_FILE, DEFAULT_POD_LABELS_FILE));
    podNameFile = new File(podInfoDir, conf.getOrDefault(POD_NAME_FILE, DEFAULT_POD_NAME_FILE));
    podUidFile = new File(podInfoDir, conf.getOrDefault(POD_UID_FILE, DEFAULT_POD_UID_FILE));
    podNamespaceFile = new File(podInfoDir, conf.getOrDefault(POD_NAMESPACE_FILE, DEFAULT_POD_NAMESPACE_FILE));
    workloadIdentityEnabled = Boolean.parseBoolean(conf.get(WORKLOAD_IDENTITY_ENABLED));
    if (workloadIdentityEnabled) {
      // Validate all workload identity configurations are set
      String missingConfig = null;
      workloadIdentityPool = conf.get(WORKLOAD_IDENTITY_POOL);
      if (workloadIdentityPool == null) {
        missingConfig = WORKLOAD_IDENTITY_POOL;
      }
      workloadIdentityProvider = conf.get(WORKLOAD_IDENTITY_PROVIDER);
      if (workloadIdentityProvider == null) {
        missingConfig = WORKLOAD_IDENTITY_PROVIDER;
      }
      workloadIdentityServiceAccountTokenTTLSeconds = WORKLOAD_IDENTITY_SERVICE_ACCOUNT_TOKEN_TTL_SECONDS_DEFAULT;
      String confWorkloadIdentityTokenTTL = conf.get(WORKLOAD_IDENTITY_SERVICE_ACCOUNT_TOKEN_TTL_SECONDS);
      if (confWorkloadIdentityTokenTTL != null) {
        workloadIdentityServiceAccountTokenTTLSeconds = Long.parseLong(confWorkloadIdentityTokenTTL);
      }
      if (workloadIdentityServiceAccountTokenTTLSeconds <= 0) {
        throw new IllegalArgumentException(String.format("Workload identity k8s service account token TTL '%d' " +
                                                           "cannot be less than zero",
                                                         workloadIdentityServiceAccountTokenTTLSeconds));
      }
      if (missingConfig != null) {
        throw new IllegalArgumentException(String.format("Missing expected workload identity config '%s'",
                                                         missingConfig));
      }
    }

    workloadLauncherRoleNameForNamespace = conf.getOrDefault(WORKLOAD_LAUNCHER_NAMESPACE_ROLE_NAME,
                                                             DEFAULT_WORKLOAD_LAUNCHER_NAMESPACE_ROLE_NAME);
    workloadLauncherRoleNameForCluster = conf.getOrDefault(WORKLOAD_LAUNCHER_CLUSTER_ROLE_NAME,
                                                           DEFAULT_WORKLOAD_LAUNCHER_CLUSTER_ROLE_NAME);

    // We don't support scaling from inside pod. Scaling should be done via CDAP operator.
    // Currently we don't support more than one instance per system service, hence set it to "1".
    conf.put(MASTER_MAX_INSTANCES, "1");
    // No TX in K8s
    conf.put(DATA_TX_ENABLED, Boolean.toString(false));

    coreV1Api = new CoreV1Api(Config.defaultClient());
    rbacV1Api = new RbacAuthorizationV1Api(Config.defaultClient());
    // Load the pod labels from the configured path. It should be setup by the CDAP operator
    podInfo = createPodInfo(conf);
    Map<String, String> podLabels = podInfo.getLabels();

    String namespace = podInfo.getNamespace();
    String cdapNamespace = conf.getOrDefault(NAMESPACE_KEY, DEFAULT_NAMESPACE);
    additionalSparkConfs = getSparkConfigurations(conf);
    programCpuMultiplier = conf.getOrDefault(PROGRAM_CPU_MULTIPLIER, DEFAULT_PROGRAM_CPU_MULTIPLIER);

    // Get the instance label to setup prefix for K8s services
    String instanceLabel = conf.getOrDefault(INSTANCE_LABEL, DEFAULT_INSTANCE_LABEL);
    String instanceName = podLabels.get(instanceLabel);
    if (instanceName == null) {
      throw new IllegalStateException("Missing instance label '" + instanceLabel + "' from pod labels.");
    }

    // Add Environment Related conf properties
    String componentName = getComponentName(podInfo.getLabels().get(instanceLabel), podInfo.getName());
    conf.put(COMPONENT_ENV_PROPERTY, componentName);
    conf.put(NAMESPACE_ENV_PROPERTY, podInfo.getNamespace());

    // Services are publish to K8s with a prefix
    String resourcePrefix = "cdap-" + instanceName + "-";
    discoveryService = new KubeDiscoveryService(cdapNamespace, "cdap-" + instanceName + "-", podLabels,
                                                podInfo.getOwnerReferences());

    // Optionally creates the pod killer task
    String podKillerSelector = conf.get(POD_KILLER_SELECTOR);
    if (!Strings.isNullOrEmpty(podKillerSelector)) {
      long delayMillis = DEFAULT_POD_KILLER_DELAY_MILLIS;
      String confDelay = conf.get(POD_KILLER_DELAY_MILLIS);
      if (!Strings.isNullOrEmpty(confDelay)) {
        try {
          delayMillis = Long.parseLong(confDelay);
          if (delayMillis <= 0) {
            delayMillis = DEFAULT_POD_KILLER_DELAY_MILLIS;
            LOG.warn("Only positive value is allowed for configuration {}. Defaulting to {}",
                     POD_KILLER_DELAY_MILLIS, delayMillis);
          }
        } catch (NumberFormatException e) {
          LOG.warn("Invalid value for configuration {}. Expected a positive integer, but get {}.",
                   POD_KILLER_DELAY_MILLIS, confDelay);
        }
      }

      podKillerTask = new PodKillerTask(namespace, podKillerSelector, delayMillis);
      LOG.info("Created pod killer task on namespace {}, with selector {} and delay {}",
               namespace, podKillerSelector, delayMillis);
    }

    String twillRunnables = context.getConfigurations().get(TWILL_RUNNER_SERVICE_MONITOR_DISABLE);
    boolean enableMonitor = true;
    if (twillRunnables != null && podLabels != null) {
      for (String twillRunnable : twillRunnables.split(",")) {
        if (podLabels.containsKey(DEFAULT_CONTAINER_LABEL) &&
          podLabels.get(DEFAULT_CONTAINER_LABEL).equals(twillRunnable)) {
          enableMonitor = false;
        }
      }
    }
    twillRunner = new KubeTwillRunnerService(context, namespace, discoveryService,
                                             podInfo, resourcePrefix,
                                             Collections.singletonMap(instanceLabel, instanceName),
                                             Integer.parseInt(conf.getOrDefault(JOB_CLEANUP_INTERVAL, "60")),
                                             Integer.parseInt(conf.getOrDefault(JOB_CLEANUP_BATCH_SIZE, "1000")),
                                             enableMonitor);
    LOG.info("Kubernetes environment initialized with pod labels {}", podLabels);
  }

  @Override
  public void destroy() {
    if (!Strings.isNullOrEmpty(configMapName)) {
      try {
        coreV1Api.deleteNamespacedConfigMap(configMapName, podInfo.getNamespace(), null, null, null, null, null, null);
      } catch (ApiException e) {
        LOG.warn("Error cleaning up configmap {}, it will be retried. {} ", configMapName, e.getResponseBody(), e);
      }
    }

    discoveryService.close();
    LOG.info("Kubernetes environment destroyed");
  }

  @Override
  public String getName() {
    return "k8s";
  }

  @Override
  public Supplier<DiscoveryService> getDiscoveryServiceSupplier() {
    return () -> discoveryService;
  }

  @Override
  public Supplier<DiscoveryServiceClient> getDiscoveryServiceClientSupplier() {
    return () -> discoveryService;
  }

  @Override
  public Supplier<TwillRunnerService> getTwillRunnerSupplier() {
    return () -> twillRunner;
  }

  @Override
  public Optional<MasterEnvironmentTask> getTask() {
    return Optional.ofNullable(podKillerTask);
  }

  @Override
  public MasterEnvironmentRunnable createRunnable(MasterEnvironmentRunnableContext context,
                                                  Class<? extends MasterEnvironmentRunnable> cls) throws Exception {
    return cls.getConstructor(MasterEnvironmentRunnableContext.class, MasterEnvironment.class)
      .newInstance(context, this);
  }

  @Override
  public SparkConfig generateSparkSubmitConfig(SparkSubmitContext sparkSubmitContext) throws Exception {
    // Get k8s master path for spark submit
    String master = kubeMasterPathProvider.getMasterPath();

    Map<String, String> sparkConfMap = new HashMap<>(additionalSparkConfs);
    // Create pod template with config maps and add to spark conf.
    sparkConfMap.put(SPARK_KUBERNETES_DRIVER_POD_TEMPLATE,
                     getDriverPodTemplate(podInfo, sparkSubmitContext).getAbsolutePath());
    sparkConfMap.put(SPARK_KUBERNETES_EXECUTOR_POD_TEMPLATE, getExecutorPodTemplateFile().getAbsolutePath());
    sparkConfMap.put(SPARK_KUBERNETES_METRICS_PROPERTIES_CONF, "/opt/spark/work-dir/metrics.properties");

    Map<String, String> submitConfigs = sparkSubmitContext.getConfig();
    String executionNamespace = submitConfigs.getOrDefault(NAMESPACE_PROPERTY, podInfo.getNamespace());
    sparkConfMap.put(SPARK_KUBERNETES_NAMESPACE, executionNamespace);

    // Set spark service account for both driver and executor to be inherited from app-fabric. Since the service account
    // is created by CDAP specifically for the namespace, it is granted reduced permissions.
    // TODO(CDAP-19149): Cleanup strong coupling currently present in CDAP service accounts to avoid copying.
    String workloadServiceAccountName = podInfo.getServiceAccountName();
    sparkConfMap.put(SPARK_KUBERNETES_DRIVER_SERVICE_ACCOUNT, workloadServiceAccountName);
    sparkConfMap.put(SPARK_KUBERNETES_EXECUTOR_SERVICE_ACCOUNT, workloadServiceAccountName);

    // Add spark driver and executor pod cpu limits: https://spark.apache.org/docs/latest/running-on-kubernetes.html
    // We are not adding memory limits because it will be same as what is requested by spark driver and executor pods.
    // On spark on kubernetes, currently there is no way to override memory limits:
    // https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/issues/783
    int driverCpuRequested =
      (int) (sparkSubmitContext.getDriverVirtualCores() * 1000 * Float.parseFloat(programCpuMultiplier));
    int driverCpuLimit = sparkSubmitContext.getDriverVirtualCores() * 1000;
    int executorCpuRequested =
      (int) (sparkSubmitContext.getExecutorVirtualCores() * 1000 * Float.parseFloat(programCpuMultiplier));
    int executorCpuLimit = sparkSubmitContext.getExecutorVirtualCores() * 1000;

    sparkConfMap.put(SPARK_DRIVER_POD_CPU_REQUEST, String.format("%dm", driverCpuRequested));
    sparkConfMap.put(SPARK_DRIVER_POD_CPU_LIMIT, String.format("%dm", driverCpuLimit));
    sparkConfMap.put(SPARK_EXECUTOR_POD_CPU_REQUEST, String.format("%dm", executorCpuRequested));
    sparkConfMap.put(SPARK_EXECUTOR_POD_CPU_LIMIT, String.format("%dm", executorCpuLimit));

    // Add spark pod labels. This will be same as job labels
    populateLabels(sparkConfMap);

    // Kube Master environment would always contain spark job jar file.
    // https://github.com/cdapio/cdap/blob/develop/cdap-spark-core3_2.12/src/k8s/Dockerfile#L46
    return new SparkConfig("k8s://" + master,
                           URI.create("local:/opt/cdap/cdap-spark-core/cdap-spark-core.jar"),
                           sparkConfMap, getPodWatcherThread());
  }

  @Override
  public void onNamespaceCreation(String cdapNamespace, Map<String, String> properties) throws Exception {
    if (NamespaceId.isReserved(cdapNamespace)) {
      return;
    }

    String namespace = properties.get(NAMESPACE_PROPERTY);
    if (namespace == null || namespace.isEmpty()) {
      throw new IOException(String.format("Cannot create Kubernetes namespace for %s because no name was provided",
                                          cdapNamespace));
    }
    // Kubernetes namespace must be a lowercase RFC 1123 label, consisting of lower case alphanumeric characters or '-'
    // and must start and end with an alphanumeric character
    KubeUtil.validateRFC1123LabelName(namespace);
    findOrCreateKubeNamespace(namespace, cdapNamespace);
    updateOrCreateResourceQuota(namespace, cdapNamespace, properties);
    copyVolumes(namespace, cdapNamespace);
    createWorkloadServiceAccount(namespace, cdapNamespace);
    if (workloadIdentityEnabled) {
      String workloadIdentityServiceAccountEmail = properties.get(WORKLOAD_IDENTITY_GCP_SERVICE_ACCOUNT_EMAIL_PROPERTY);
      if (workloadIdentityServiceAccountEmail != null && !workloadIdentityServiceAccountEmail.isEmpty()) {
        findOrCreateWorkloadIdentityConfigMap(namespace, workloadIdentityServiceAccountEmail);
      }
    }
  }

  @Override
  public void onNamespaceDeletion(String cdapNamespace, Map<String, String> properties) throws Exception {
    String namespace = properties.get(NAMESPACE_PROPERTY);
    if (namespace != null && !namespace.isEmpty()) {
      deleteKubeNamespace(namespace, cdapNamespace);
    }
  }

  /**
   * <p>Parses k8s pod name to find name of CDAP service running in this pod by removing prefix "cdap-"
   * and instance name from the pod name. Eg: pod name "cdap-abc-metrics-0" and instance name "abc"
   * will return component name "metrics-0". </p>
   *
   * @param instanceName Name of CDAP instance
   * @param podName      Name of K8s pod in which this service is running
   * @return componentName after parsing pod name
   */
  @VisibleForTesting
  static String getComponentName(String instanceName, String podName) {
    String componentName = podName;
    // remove prefix "cdap-"
    componentName = componentName.replaceFirst("^(cdap-)", "");
    // remove instance name
    componentName = componentName.replaceFirst(Pattern.quote(instanceName), "");
    // Strip "-" from from beginning.
    componentName = componentName.replaceAll("^-+", "");
    return componentName;
  }

  /**
   * Returns the {@link PodInfo} of the current environment.
   */
  public PodInfo getPodInfo() {
    if (podInfo == null) {
      throw new IllegalStateException("This environment is not yet initialized");
    }
    return podInfo;
  }

  private PodInfo createPodInfo(Map<String, String> conf) throws IOException, ApiException {
    String systemNamespace = conf.getOrDefault(NAMESPACE_KEY, DEFAULT_NAMESPACE);

    if (!podInfoDir.isDirectory()) {
      throw new IllegalArgumentException(String.format("%s is not a directory.", podInfoDir.getAbsolutePath()));
    }
    String namespace = podNamespaceFile.exists() ?
      Files.lines(podNamespaceFile.toPath()).findFirst().orElse(null) : systemNamespace;

    // Load the pod labels from the configured path. It should be setup by the CDAP operator
    Map<String, String> podLabels = new HashMap<>();
    try (BufferedReader reader = Files.newBufferedReader(podLabelsFile.toPath(), StandardCharsets.UTF_8)) {
      String line = reader.readLine();
      while (line != null) {
        Matcher matcher = LABEL_PATTERN.matcher(line);
        if (matcher.matches()) {
          podLabels.put(matcher.group(1), matcher.group(2));
        }
        // Use namespace from podLabels instead if it exists
        Matcher namespaceMatcher = NAMESPACE_LABEL_PATTERN.matcher(line);
        if (namespaceMatcher.matches()) {
          namespace = namespaceMatcher.group(2);
          podLabels.put(namespaceMatcher.group(1), namespaceMatcher.group(2));
        }
        line = reader.readLine();
      }
    }

    String podName = Files.lines(podNameFile.toPath()).findFirst().orElse(null);
    if (Strings.isNullOrEmpty(podName)) {
      throw new IOException("Failed to get pod name from file " + podNameFile);
    }

    String podUid = Files.lines(podUidFile.toPath()).findFirst().orElse(null);
    if (Strings.isNullOrEmpty(podUid)) {
      throw new IOException("Failed to get pod uid from file " + podUidFile);
    }

    // Query pod information.
    V1Pod pod;
    try {
      pod = coreV1Api.readNamespacedPod(podName, namespace, null, null, null);
    } catch (ApiException e) {
      throw new IOException("Error occurred while getting pod. Error code = "
                              + e.getCode() + ", Body = " + e.getResponseBody(), e);
    }
    V1ObjectMeta podMeta = pod.getMetadata();
    List<V1OwnerReference> ownerReferences = podMeta == null || podMeta.getOwnerReferences() == null ?
      Collections.emptyList() : podMeta.getOwnerReferences();

    // Find the container that is having this CDAP process running inside (because a pod can have multiple containers).
    // If there is no such label, default to the first container.
    // The name of the label will be used to hold the name of new container created by this process.
    // We use the same label name so that we don't need to alter the configuration for new pod
    String containerLabelName = conf.getOrDefault(CONTAINER_LABEL, DEFAULT_CONTAINER_LABEL);
    String containerName = podLabels.get(containerLabelName);
    V1Container container = pod.getSpec().getContainers().stream()
      .filter(c -> Objects.equals(containerName, c.getName()))
      .findFirst()
      .orElse(pod.getSpec().getContainers().get(0));

    // Get the config volumes from the pod
    List<V1Volume> volumes = pod.getSpec().getVolumes().stream()
      .filter(v -> CONFIG_NAMES.contains(v.getName()) || isCustomVolumePrefix(v.getName()))
      .collect(Collectors.toList());

    // Get the volume mounts from the container
    List<V1VolumeMount> mounts = container.getVolumeMounts().stream()
      .filter(m -> CONFIG_NAMES.contains(m.getName()) || isCustomVolumePrefix(m.getName()))
      .collect(Collectors.toList());

    List<V1EnvVar> envs = container.getEnv();

    // Use the same service account and the runtime class as the current process for now.
    // Ideally we should use a more restricted role.
    String serviceAccountName = pod.getSpec().getServiceAccountName();
    String runtimeClassName = pod.getSpec().getRuntimeClassName();
    return new PodInfo(podName, podLabelsFile.getParentFile().getAbsolutePath(), podLabelsFile.getName(),
                       podNameFile.getName(), podUid, podUidFile.getName(), podNamespaceFile.getName(),
                       namespace, podLabels, ownerReferences,
                       serviceAccountName, runtimeClassName,
                       volumes, containerLabelName, container.getImage(), mounts,
                       envs == null ? Collections.emptyList() : envs, pod.getSpec().getSecurityContext(),
                       container.getImagePullPolicy());
  }

  /**
   * Returns {@code true} if the given volume name is prefixed with the custom volume mapping from the CRD.
   */
  private boolean isCustomVolumePrefix(String name) {
    return CUSTOM_VOLUME_PREFIX.stream().anyMatch(name::startsWith);
  }

  /**
   * Adds secrets from the host pod as volume and volume mounts.
   * TODO(CDAP-19400): Remove this logic when decoupling privileged and unprivileged workflows.
   *
   * @param podInfo Host pod info
   * @param podSpec Pod specification to mount secret volumes to
   */
  private void mountHostSecretVolumes(PodInfo podInfo, V1PodSpec podSpec) {
    Set<String> secretVolumeNames = new HashSet<>();
    List<V1VolumeMount> secretVolumeMounts = new ArrayList<>();
    for (V1Volume volume : podInfo.getVolumes()) {
      if (volume.getSecret() != null) {
        secretVolumeNames.add(volume.getName());
        podSpec.addVolumesItem(volume);
      }
    }
    // Mount volumes to all containers.
    for (V1VolumeMount volumeMount : podInfo.getContainerVolumeMounts()) {
      if (secretVolumeNames.contains(volumeMount.getName())) {
        for (V1Container container : podSpec.getContainers()) {
          container.addVolumeMountsItem(volumeMount);
        }
      }
    }
  }

  private File getDriverPodTemplate(PodInfo podInfo, SparkSubmitContext sparkSubmitContext) throws Exception {
    V1Pod driverPod = new V1Pod();
    // set owner references for driver pod
    driverPod.setMetadata(new V1ObjectMetaBuilder()
                            .addToOwnerReferences(new V1OwnerReferenceBuilder()
                                                    .withBlockOwnerDeletion(true)
                                                    .withKind("Pod")
                                                    .withApiVersion("v1")
                                                    .withUid(podInfo.getUid())
                                                    .withName(podInfo.getName()).build()).build());
    V1PodSpec driverPodSpec = createBasePodSpec();
    driverPod.setSpec(driverPodSpec);

    // While generating driver pod template, need to create config map that has compressed localize files
    try {
      V1ConfigMapBuilder configMapBuilder = new V1ConfigMapBuilder();
      // create config map with localize resources
      for (Map.Entry<String, SparkLocalizeResource> resource : sparkSubmitContext.getLocalizeResources().entrySet()) {
        String fileName = resource.getValue().isArchive() ? resource.getKey() : resource.getKey() + ".zip";
        configMapBuilder.addToBinaryData(fileName, getContents(resource.getValue().getURI(),
                                                               !resource.getValue().isArchive()));
      }

      String configMapName = CDAP_CONFIG_MAP_PREFIX + UUID.randomUUID();
      coreV1Api.createNamespacedConfigMap(podInfo.getNamespace(),
                                          configMapBuilder.withMetadata(
                                            new V1ObjectMetaBuilder()
                                              .withName(configMapName)
                                              .withLabels(podInfo.getLabels())
                                              .withOwnerReferences(podInfo.getOwnerReferences())
                                              .build()).build(),
                                          null, null, null);
      this.configMapName = configMapName;

      // Add configmap and secrets as a volume to be added to the pod template
      driverPodSpec.addVolumesItem(new V1Volume().name(configMapName)
                                     .configMap(new V1ConfigMapVolumeSourceBuilder().withName(configMapName).build()));
      // Add configmap and secrets as a volume mount
      for (V1Container container : driverPodSpec.getContainers()) {
        container.addVolumeMountsItem(new V1VolumeMount().name(configMapName)
                                        .mountPath(CDAP_LOCALIZE_FILES_PATH).readOnly(true));
      }

      // TODO(CDAP-19400): Remove this logic when decoupling privileged and unprivileged workflows.
      mountHostSecretVolumes(podInfo, driverPodSpec);

      if (workloadIdentityEnabled) {
        setupWorkloadIdentityForPodSpec(driverPodSpec, workloadIdentityPool,
                                        workloadIdentityServiceAccountTokenTTLSeconds);
      }
    } catch (ApiException e) {
      throw new IOException("Error occurred while creating pod spec. Error code = "
                              + e.getCode() + ", Body = " + e.getResponseBody(), e);
    } catch (IOException e) {
      throw new IOException("Error while creating compressed files to generate pod spec.", e);
    }

    return serializePodTemplate(driverPod);
  }

  private File getExecutorPodTemplateFile() throws Exception {
    V1Pod executorPod = new V1Pod();
    V1PodSpec executorPodSpec = createBasePodSpec();
    executorPod.setSpec(executorPodSpec);

    // Add configmap as a volume
    executorPodSpec.addVolumesItem(new V1Volume().name(configMapName)
                                     .configMap(new V1ConfigMapVolumeSourceBuilder().withName(configMapName).build()));
    // Add configmap as a volume mount
    for (V1Container container : executorPodSpec.getContainers()) {
      container.addVolumeMountsItem(new V1VolumeMount().name(configMapName)
                                      .mountPath(CDAP_LOCALIZE_FILES_PATH).readOnly(true));
    }

    // TODO(CDAP-19400): Remove this logic when decoupling privileged and unprivileged workflows.
    mountHostSecretVolumes(podInfo, executorPodSpec);

    if (workloadIdentityEnabled) {
      setupWorkloadIdentityForPodSpec(executorPodSpec, workloadIdentityPool,
                                      workloadIdentityServiceAccountTokenTTLSeconds);
    }

    // Create spark template file. We do not delete it because pod will get deleted at the end of job completion.
    return serializePodTemplate(executorPod);
  }

  private File serializePodTemplate(V1Pod v1Pod) throws IOException {
    // Uses a relative path to create the Spark driver and executor template files.
    File templateFile = localFileProvider.getWritableFileRef(POD_TEMPLATE_FILE_NAME + UUID.randomUUID());
    String podTemplateYaml = Yaml.dump(v1Pod);
    try (FileWriter writer = new FileWriter(templateFile)) {
      writer.write(podTemplateYaml);
    } catch (IOException e) {
      // should not happen
      throw new IOException("Exception while writing pod spec to temp file.", e);
    }
    LOG.trace("Pod template: {}", podTemplateYaml);
    return templateFile;
  }

  /**
   * Creates the base pod spec to use as a template for all pods that Spark creates.
   *
   * @return pod template to use for Spark pods
   */
  private V1PodSpec createBasePodSpec() {
    /*
        define the podinfo volume. This uses downwardAPI, which tells k8s to store pod information in files:

        - downwardAPI:
          defaultMode: 420
          items:
          - fieldRef:
              fieldPath: metadata.labels
            path: pod.labels.properties
          - fieldRef:
              fieldPath: metadata.name
            path: pod.name
          - fieldRef:
              fieldPath: metadata.uid
            path: pod.uid
          - fieldRef:
              fieldPath: metadata.namespace
            path: pod.namespace
        name: podinfo
     */
    V1PodSpecBuilder v1PodSpecBuilder = new V1PodSpecBuilder();

    List<V1Volume> volumes = new ArrayList<>();
    V1DownwardAPIVolumeFile labelsFile = new V1DownwardAPIVolumeFile()
      .fieldRef(new V1ObjectFieldSelector().fieldPath("metadata.labels"))
      .path(podLabelsFile.getName());
    V1DownwardAPIVolumeFile nameFile = new V1DownwardAPIVolumeFile()
      .fieldRef(new V1ObjectFieldSelector().fieldPath("metadata.name"))
      .path(podNameFile.getName());
    V1DownwardAPIVolumeFile uidFile = new V1DownwardAPIVolumeFile()
      .fieldRef(new V1ObjectFieldSelector().fieldPath("metadata.uid"))
      .path(podUidFile.getName());
    V1DownwardAPIVolumeFile namespaceFile = new V1DownwardAPIVolumeFile()
      .fieldRef(new V1ObjectFieldSelector().fieldPath("metadata.namespace"))
      .path(podNamespaceFile.getName());
    V1DownwardAPIVolumeSource podinfoVolume = new V1DownwardAPIVolumeSource()
      .defaultMode(420)
      .items(Arrays.asList(labelsFile, nameFile, uidFile, namespaceFile));
    String podInfoVolumeName = "podinfo";
    volumes.add(new V1Volume().downwardAPI(podinfoVolume).name(podInfoVolumeName));

    List<V1VolumeMount> volumeMounts = new ArrayList<>();
    volumeMounts.add(new V1VolumeMount().name(podInfoVolumeName).mountPath(podInfoDir.getAbsolutePath()));

    v1PodSpecBuilder
      .withVolumes(volumes)
      .withContainers(new V1ContainerBuilder()
                        .withVolumeMounts(volumeMounts)
                        .build());
    return v1PodSpecBuilder.build();
  }

  private void populateLabels(Map<String, String> sparkConfMap) {
    for (Map.Entry<String, String> label : podInfo.getLabels().entrySet()) {
      if (label.getKey().equals(CDAP_CONTAINER_LABEL)) {
        // Make sure correct container name label is being added for driver and executor containers
        sparkConfMap.put(SPARK_KUBERNETES_DRIVER_LABEL_PREFIX + label.getKey(),
                         SPARK_KUBERNETES_DRIVER_CONTAINER_VALUE);
        sparkConfMap.put(SPARK_KUBERNETES_EXECUTOR_LABEL_PREFIX + label.getKey(),
                         SPARK_KUBERNETES_EXECUTOR_CONTAINER_VALUE);
      } else {
        sparkConfMap.put(SPARK_KUBERNETES_DRIVER_LABEL_PREFIX + label.getKey(), label.getValue());
        sparkConfMap.put(SPARK_KUBERNETES_EXECUTOR_LABEL_PREFIX + label.getKey(), label.getValue());
      }
    }
  }

  private byte[] getContents(URI uri, boolean shouldCompress) throws IOException {
    byte[] buffer = new byte[1024 * 500]; // use 500kb buffer
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    File file = new File(uri);
    try (OutputStream os = shouldCompress ? new GZIPOutputStream(baos) : baos;
         FileInputStream fis = new FileInputStream(file)) {
      int length;
      while ((length = fis.read(buffer)) > 0) {
        os.write(buffer, 0, length);
      }
    }
    return baos.toByteArray();
  }

  private Map<String, String> getSparkConfigurations(Map<String, String> cConf) {
    Map<String, String> sparkConfs = new HashMap<>();
    for (Map.Entry<String, String> entry : cConf.entrySet()) {
      if (entry.getKey().startsWith(SPARK_CONFIGS_PREFIX)) {
        sparkConfs.put(entry.getKey(), entry.getValue());
      }
    }
    return sparkConfs;
  }

  /**
   * Checks if namespace already exists from the same CDAP instance. Otherwise, creates a new Kubernetes namespace.
   */
  private void findOrCreateKubeNamespace(String namespace, String cdapNamespace) throws Exception {
    try {
      V1Namespace existingNamespace = coreV1Api.readNamespace(namespace, null, null, null);
      if (existingNamespace.getMetadata() == null) {
        throw new IOException(String.format("Kubernetes namespace %s exists but was not created by CDAP", namespace));
      }
      Map<String, String> labels = existingNamespace.getMetadata().getLabels();
      if (labels == null || !cdapNamespace.equals(labels.get(CDAP_NAMESPACE_LABEL))) {
        throw new IOException(String.format("Kubernetes namespace %s exists but was not created by CDAP namespace %s",
                                            namespace, cdapNamespace));
      }
    } catch (ApiException e) {
      if (e.getCode() != HttpURLConnection.HTTP_NOT_FOUND) {
        throw new IOException("Error occurred while checking if Kubernetes namespace already exists. Error code = "
                                + e.getCode() + ", Body = " + e.getResponseBody(), e);
      }
      createKubeNamespace(namespace, cdapNamespace);
    }
  }

  private void createKubeNamespace(String namespace, String cdapNamespace) throws Exception {
    V1Namespace namespaceObject = new V1Namespace();
    namespaceObject.setMetadata(new V1ObjectMeta().name(namespace).putLabelsItem(CDAP_NAMESPACE_LABEL, cdapNamespace));
    try {
      coreV1Api.createNamespace(namespaceObject, null, null, null);
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
   * Updates resource quota if it already exists in the Kubernetes namespace. Otherwise, creates a new resource quota.
   */
  private void updateOrCreateResourceQuota(String namespace, String cdapNamespace, Map<String, String> properties)
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
      V1ResourceQuota existingResourceQuota = coreV1Api.readNamespacedResourceQuota(RESOURCE_QUOTA_NAME, namespace,
                                                                                    null, null, null);
      if (existingResourceQuota.getMetadata() == null) {
        throw new IOException(String.format("%s exists but was not created by CDAP", RESOURCE_QUOTA_NAME));
      }
      Map<String, String> labels = existingResourceQuota.getMetadata().getLabels();
      if (labels == null || !cdapNamespace.equals(labels.get(CDAP_NAMESPACE_LABEL))) {
        throw new IOException(String.format("%s exists but was not created by CDAP namespace %s",
                                            RESOURCE_QUOTA_NAME, cdapNamespace));
      }
      if (!hardLimitMap.equals(existingResourceQuota.getSpec().getHard())) {
        coreV1Api.replaceNamespacedResourceQuota(RESOURCE_QUOTA_NAME, namespace, resourceQuota, null, null, null);
      }
    } catch (ApiException e) {
      if (e.getCode() != HttpURLConnection.HTTP_NOT_FOUND) {
        throw new IOException("Error occurred while checking or updating Kubernetes resource quota. Error code = "
                                + e.getCode() + ", Body = " + e.getResponseBody(), e);
      }
      createKubeResourceQuota(namespace, resourceQuota);
    }
  }

  private void createKubeResourceQuota(String namespace, V1ResourceQuota resourceQuota) throws Exception {
    try {
      coreV1Api.createNamespacedResourceQuota(namespace, resourceQuota, null, null, null);
      LOG.debug("Created resource quota for Kubernetes namespace {}", namespace);
    } catch (ApiException e) {
      throw new IOException("Error occurred while creating Kubernetes resource quota. Error code = "
                              + e.getCode() + ", Body = " + e.getResponseBody(), e);
    }
  }

  /**
   * Copy volumes into the new namespace for deployments created via the KubeTwillRunnerService
   * TODO: (CDAP-18956) improve this logic to be for each pipeline run
   */
  private void copyVolumes(String namespace, String cdapNamespace) throws IOException {
    try {
      for (V1Volume volume : podInfo.getVolumes()) {
        if (volume.getConfigMap() != null) {
          String configMapName = volume.getConfigMap().getName();
          V1ConfigMap existingMap = coreV1Api.readNamespacedConfigMap(configMapName, podInfo.getNamespace(),
                                                                      null, null, null);
          V1ConfigMap configMap = new V1ConfigMap().data(existingMap.getData())
            .metadata(new V1ObjectMeta().name(configMapName).putLabelsItem(CDAP_NAMESPACE_LABEL, cdapNamespace));
          try {
            coreV1Api.createNamespacedConfigMap(namespace, configMap, null, null, null);
          } catch (ApiException e) {
            if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
              throw e;
            }
            LOG.warn("The configmap already exists '{}:{}' : {}. Ignoring creation of the configmap.", namespace,
                     configMapName, e.getResponseBody());
          }
          LOG.debug("Created configMap {} in Kubernetes namespace {}", configMapName, namespace);
        }

        if (volume.getSecret() != null) {
          String secretName = volume.getSecret().getSecretName();
          V1Secret existingSecret = coreV1Api.readNamespacedSecret(secretName, podInfo.getNamespace(),
                                                                   null, null, null);
          V1Secret secret = new V1Secret().data(existingSecret.getData()).type(existingSecret.getType())
            .metadata(new V1ObjectMeta().name(secretName).putLabelsItem(CDAP_NAMESPACE_LABEL, cdapNamespace));
          try {
            coreV1Api.createNamespacedSecret(namespace, secret, null, null, null);
          } catch (ApiException e) {
            if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
              throw e;
            }
            LOG.warn("The secret '{}:{}' already exists : {}. Ignoring creation of the secret.", namespace,
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
  private void createWorkloadServiceAccount(String namespace, String cdapNamespace) throws IOException {
    try {
      // Create service account for workload pod
      // TODO(CDAP-19149): Cleanup strong coupling currently present in CDAP service accounts to avoid copying.
      String serviceAccountName = podInfo.getServiceAccountName();
      createServiceAccount(namespace, cdapNamespace, serviceAccountName);

      // Create namespace-specific role-binding for the workload service account
      createNamespacedRoleBinding(WORKLOAD_LAUNCHER_NAMESPACE_ROLE_BINDING_NAME, CLUSTER_ROLE_KIND,
                                  workloadLauncherRoleNameForNamespace, namespace, serviceAccountName, cdapNamespace);

      // Create cluster-wide role-binding for the workload service account
      String workloadLauncherClusterRoleBindingName = String.format(WORKLOAD_LAUNCHER_CLUSTER_ROLE_BINDING_FORMAT,
                                                                    namespace);
      createClusterRoleBinding(workloadLauncherClusterRoleBindingName, workloadLauncherRoleNameForCluster, namespace,
                               serviceAccountName, cdapNamespace);

    } catch (ApiException e) {
      throw new IOException("Error occurred while creating service account or role binding. Error code = "
                              + e.getCode() + ", Body = " + e.getResponseBody(), e);
    }
  }

  private void createServiceAccount(String namespace, String cdapNamespace, String serviceAccountName)
    throws ApiException {
    V1ServiceAccount serviceAccount = new V1ServiceAccount()
      .metadata(new V1ObjectMeta().name(serviceAccountName)
                  .putLabelsItem(CDAP_NAMESPACE_LABEL, cdapNamespace));
    try {
      coreV1Api.createNamespacedServiceAccount(namespace, serviceAccount, null, null, null);
    } catch (ApiException e) {
      if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
        throw e;
      }
      LOG.warn("The service account '{}:{}' already exists : {}. Ignoring creation of the service account.", namespace,
               serviceAccountName, e.getResponseBody());
    }
    LOG.info("Created serviceAccount {} in Kubernetes namespace {}", serviceAccountName, namespace);
  }

  private void createNamespacedRoleBinding(String bindingName, String roleKind, String roleName, String namespace,
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
      rbacV1Api.createNamespacedRoleBinding(namespace, namespaceWorkloadLauncherBinding, null, null, null);
    } catch (ApiException e) {
      if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
        throw e;
      }
      LOG.warn("The role binding '{}:{}' already exists : {}. Ignoring creation of the role binding.", namespace,
               bindingName, e.getResponseBody());
    }

    LOG.info("Created namespace role binding '{}' in k8s namespace '{}' for service account '{}'",
             bindingName, serviceAccountName, serviceAccountName);
  }

  private void createClusterRoleBinding(String bindingName, String roleName, String serviceAccountNamespace,
                                        String serviceAccountName, String cdapNamespace) throws ApiException {
    KubeUtil.validatePathSegmentName(bindingName);
    V1ClusterRoleBinding clusterWorkloadLauncherBinding = new V1ClusterRoleBindingBuilder()
      .withMetadata(new V1ObjectMetaBuilder()
                      .withName(bindingName).build().putLabelsItem(CDAP_NAMESPACE_LABEL, cdapNamespace))
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
      rbacV1Api.createClusterRoleBinding(clusterWorkloadLauncherBinding, null, null, null);
    } catch (ApiException e) {
      if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
        throw e;
      }
      LOG.warn("The cluster role binding '{}' already exists : {}. Ignoring creation of the cluster role binding.",
               bindingName, e.getResponseBody());
    }

    LOG.info("Created cluster role binding '{}' for service account '{}' in k8s namespace '{}'", bindingName,
             serviceAccountName, serviceAccountNamespace);
  }

  /**
   * Deletes Kubernetes namespace created by CDAP and associated resources if they exist.
   */
  private void deleteKubeNamespace(String namespace, String cdapNamespace) throws Exception {
    try {
      V1Namespace namespaceObject = coreV1Api.readNamespace(namespace, null, null, null);
      if (namespaceObject.getMetadata() != null) {
        Map<String, String> namespaceLabels = namespaceObject.getMetadata().getLabels();
        if (namespaceLabels != null && namespaceLabels.get(CDAP_NAMESPACE_LABEL).equals(cdapNamespace)) {
          // PropagationPolicy is set to background cascading deletion. Kubernetes deletes the owner object immediately
          // and the controller cleans up the dependent objects in the background.
          coreV1Api.deleteNamespace(namespace, null, null, 0, null, "Background", null);
          LOG.info("Deleted Kubernetes namespace and associated resources for {}", namespace);
          return;
        }
      }
      LOG.debug("Kubernetes namespace {} was not deleted because it was not created by CDAP namespace {}",
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

  /**
   * Finds or creates the ConfigMap which stores the GCP credentials for Fleet Workload Identity.
   * For details, see steps 6-7 of
   * https://cloud.google.com/anthos/multicluster-management/fleets/workload-identity#impersonate_a_service_account
   */
  private void findOrCreateWorkloadIdentityConfigMap(String k8sNamespace, String workloadIdentityGCPServiceAccountEmail)
    throws ApiException, IOException {
    // Check if workload identity config map already exists
    try {
      coreV1Api.readNamespacedConfigMap(k8sNamespace, WORKLOAD_IDENTITY_CONFIGMAP_NAME, null, false, false);
      // Workload identity config map already exists, so return early
      LOG.debug("Workload identity config found, returning without creating it...");
      return;
    } catch (ApiException e) {
      if (e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        LOG.debug("Creating workload identity config map for kubernetes namespace {}", k8sNamespace);
      } else {
        throw new IOException("Failed to fetch existing workload identity config map. Error code = " + e.getCode() +
                                ", Body = " + e.getResponseBody(), e);
      }
    }

    String workloadIdentityAudience = String.format(WORKLOAD_IDENTITY_AUDIENCE_FORMAT, workloadIdentityPool,
                                                    workloadIdentityProvider);
    String workloadIdentityImpersonationURL = String.format(WORKLOAD_IDENTITY_IMPERSONATION_URL_FORMAT,
                                                            workloadIdentityGCPServiceAccountEmail);
    GCPWorkloadIdentityCredential credential =
      new GCPWorkloadIdentityCredential(GCPWorkloadIdentityCredential.CredentialType.EXTERNAL_ACCOUNT,
                                        workloadIdentityAudience, workloadIdentityImpersonationURL,
                                        GCPWorkloadIdentityCredential.TokenType.JWT, WORKLOAD_IDENTITY_TOKEN_URL,
                                        WORKLOAD_IDENTITY_CREDENTIAL_KSA_SOURCE_PATH);
    String workloadIdentityCredentialJSON = gson.toJson(credential);
    Map<String, String> workloadIdentityConfigMapData = new HashMap<>();
    workloadIdentityConfigMapData.put(WORKLOAD_IDENTITY_DATA_KEY, workloadIdentityCredentialJSON);
    V1ConfigMap configMap = new V1ConfigMap()
      .metadata(new V1ObjectMeta().namespace(k8sNamespace).name(WORKLOAD_IDENTITY_CONFIGMAP_NAME))
      .data(workloadIdentityConfigMapData);
    coreV1Api.createNamespacedConfigMap(k8sNamespace, configMap, null, null, null);
  }

  /**
   * Applies workload identity configurations to a given pod specification. For additional details, see steps 6-7 of
   * https://cloud.google.com/anthos/multicluster-management/fleets/workload-identity#impersonate_a_service_account
   *
   * @param podSpec The pod spec to setup workload identity for.
   */
  private static void setupWorkloadIdentityForPodSpec(V1PodSpec podSpec, String workloadIdentityPool,
                                                      long workloadIdentityServiceAccountTokenTTLSeconds) {
    // Mount volume to expected directory
    V1ServiceAccountTokenProjection serviceAccountTokenProjection = new V1ServiceAccountTokenProjection()
      .path(WORKLOAD_IDENTITY_CREDENTIAL_KSA_PATH)
      .expirationSeconds(workloadIdentityServiceAccountTokenTTLSeconds)
      .audience(workloadIdentityPool);
    V1ConfigMapProjection configMapProjection = new V1ConfigMapProjection()
      .name(WORKLOAD_IDENTITY_CONFIGMAP_NAME)
      .optional(false)
      .addItemsItem(new V1KeyToPath().key(WORKLOAD_IDENTITY_CONFIGMAP_KEY)
                      .path(WORKLOAD_IDENTITY_CONFIGMAP_FILE));
    podSpec.addVolumesItem(new V1Volume().name(WORKLOAD_IDENTITY_PROJECTED_VOLUME_NAME)
                             .projected(new V1ProjectedVolumeSource()
                                          .defaultMode(420)
                                          .addSourcesItem(new V1VolumeProjection()
                                                            .serviceAccountToken(serviceAccountTokenProjection))
                                          .addSourcesItem(new V1VolumeProjection()
                                                            .configMap(configMapProjection))));
    for (V1Container container : podSpec.getContainers()) {
      // Mount projected volume
      container.addVolumeMountsItem(new V1VolumeMount().name(WORKLOAD_IDENTITY_PROJECTED_VOLUME_NAME)
                                      .mountPath(WORKLOAD_IDENTITY_CREDENTIAL_DIR).readOnly(true));
      // Setup environment variables
      container.addEnvItem(new V1EnvVar().name("GOOGLE_APPLICATION_CREDENTIALS")
                             .value(WORKLOAD_IDENTITY_CREDENTIAL_GSA_SOURCE_PATH));
    }
  }

  /**
   * Validates resource requests and limits so that limit is not lower than request.
   */
  private void validateResources(int driverCpuRequested, int driverCpuLimit,
                                 int executorCpuRequested, int executorCpuLimit) throws Exception {
    if (driverCpuLimit < driverCpuRequested) {
      throw new Exception(String.format("CPU limits %d for spark driver pod is lower than requested cpu %d",
                                        driverCpuLimit, driverCpuRequested));
    }

    if (executorCpuLimit < executorCpuRequested) {
      throw new Exception(String.format("CPU limits %d for spark executor pod is lower than requested cpu %d",
                                        executorCpuLimit, executorCpuRequested));
    }
  }

  private SparkDriverWatcher getPodWatcherThread() {
    // Start watch for driver pod. This is added because of bug in spark implementation for driver pod status.
    // Check CDAP-18511 for details.
    Map<String, String> labels = new HashMap<>(podInfo.getLabels());
    // Spark label added by kubernetes
    labels.put(SPARK_ROLE_LABEL, SPARK_DRIVER_LABEL_VALUE);
    labels.put(CDAP_CONTAINER_LABEL, SPARK_KUBERNETES_DRIVER_CONTAINER_VALUE);
    String labelSelector = labels.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue())
      .collect(Collectors.joining(","));

    return new PodWatcherThread(podInfo.getNamespace(), labelSelector);
  }

  private static class PodWatcherThread extends AbstractWatcherThread<V1Pod> implements SparkDriverWatcher {
    private final CompletableFuture<Boolean> podStatusFuture;
    private final String labelSelector;

    PodWatcherThread(String namespace, String labelSelector) {
      super("kube-pod-watcher", namespace, "", "v1", "pods");
      this.podStatusFuture = new CompletableFuture<>();
      this.labelSelector = labelSelector;
    }

    @Override
    public void initialize() {
      this.setDaemon(true);
      this.start();
    }

    @Override
    public void resourceModified(V1Pod resource) {
      if (resource.getStatus() != null && resource.getStatus().getPhase() != null) {
        // We will complete future based on terminal states of the pod:
        // https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase
        if (resource.getStatus().getPhase().equalsIgnoreCase("Succeeded")) {
          podStatusFuture.complete(true);
        } else if (resource.getStatus().getPhase().equalsIgnoreCase("Failed") ||
          resource.getStatus().getPhase().equalsIgnoreCase("Unknown")) {
          podStatusFuture.completeExceptionally(
            new Throwable(String.format("Spark pod %s returned error state.", resource.getMetadata().getName())));
        }
      }
    }

    @Override
    public void resourceDeleted(V1Pod resource) {
      // if resource was added, and then removed without going into terminal state, we mark future to be success.
      podStatusFuture.complete(true);
    }

    @Override
    protected void updateListOptions(ListOptions options) {
      options.setLabelSelector(labelSelector);
    }

    @Override
    public Future<Boolean> waitForFinish() {
      return podStatusFuture;
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
  void setLocalFileProvider(LocalFileProvider localFileProvider) {
    this.localFileProvider = localFileProvider;
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

  @VisibleForTesting
  void setWorkloadIdentityServiceAccountTokenTTLSeconds(long workloadIdentityServiceAccountTokenTTLSeconds) {
    this.workloadIdentityServiceAccountTokenTTLSeconds = workloadIdentityServiceAccountTokenTTLSeconds;
  }

  @VisibleForTesting
  void setKubeMasterPathProvider(KubeMasterPathProvider kubeMasterPathProvider) {
    this.kubeMasterPathProvider = kubeMasterPathProvider;
  }

  @VisibleForTesting
  void setAdditionalSparkConfs(Map<String, String> additionalSparkConfs) {
    this.additionalSparkConfs = additionalSparkConfs;
  }

  @VisibleForTesting
  void setPodInfo(PodInfo podInfo) {
    this.podInfo = podInfo;
  }

  @VisibleForTesting
  void setPodInfoDir(File podInfoDir) {
    this.podInfoDir = podInfoDir;
  }

  @VisibleForTesting
  void setPodLabelsFile(File podLabelsFile) {
    this.podLabelsFile = podLabelsFile;
  }

  @VisibleForTesting
  void setPodNameFile(File podNameFile) {
    this.podNameFile = podNameFile;
  }

  @VisibleForTesting
  void setPodNamespaceFile(File podNamespaceFile) {
    this.podNamespaceFile = podNamespaceFile;
  }

  @VisibleForTesting
  void setPodUidFile(File podUidFile) {
    this.podUidFile = podUidFile;
  }

  @VisibleForTesting
  void setProgramCpuMultiplier(String programCpuMultiplier) {
    this.programCpuMultiplier = programCpuMultiplier;
  }
}
