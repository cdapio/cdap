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
import io.cdap.cdap.k8s.discovery.KubeDiscoveryService;
import io.cdap.cdap.k8s.runtime.KubeTwillRunnerService;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnable;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnableContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentTask;
import io.cdap.cdap.master.spi.environment.spark.SparkConfig;
import io.cdap.cdap.master.spi.environment.spark.SparkLocalizeResource;
import io.cdap.cdap.master.spi.environment.spark.SparkSubmitContext;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMapBuilder;
import io.kubernetes.client.openapi.models.V1ConfigMapVolumeSourceBuilder;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerBuilder;
import io.kubernetes.client.openapi.models.V1DownwardAPIVolumeFile;
import io.kubernetes.client.openapi.models.V1DownwardAPIVolumeSource;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1OwnerReferenceBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodSpecBuilder;
import io.kubernetes.client.openapi.models.V1ResourceQuota;
import io.kubernetes.client.openapi.models.V1ResourceQuotaSpec;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Yaml;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
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
  private static final Logger LOG = LoggerFactory.getLogger(KubeMasterEnvironment.class);

  @VisibleForTesting
  static final String NAMESPACE_PROPERTY = "k8s.namespace";
  static final String NAMESPACE_CPU_LIMIT_PROPERTY = "k8s.namespace.cpu.limits";
  static final String NAMESPACE_MEMORY_LIMIT_PROPERTY = "k8s.namespace.memory.limits";

  // Contains the list of configuration / secret names coming from the Pod information, which are
  // needed to propagate to deployments created via the KubeTwillRunnerService
  private static final Set<String> CONFIG_NAMES = ImmutableSet.of("cdap-conf", "hadoop-conf");
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
  private static final String POD_KILLER_SELECTOR = "master.environment.k8s.pod.killer.selector";
  private static final String POD_KILLER_DELAY_MILLIS = "master.environment.k8s.pod.killer.delay.millis";
  private static final String SPARK_CONFIGS_PREFIX = "spark.kubernetes";
  private static final String SPARK_KUBERNETES_DRIVER_LABEL_PREFIX = "spark.kubernetes.driver.label.";
  private static final String SPARK_KUBERNETES_EXECUTOR_LABEL_PREFIX = "spark.kubernetes.executor.label.";
  private static final String SPARK_KUBERNETES_DRIVER_POD_TEMPLATE = "spark.kubernetes.driver.podTemplateFile";
  private static final String SPARK_KUBERNETES_EXECUTOR_POD_TEMPLATE = "spark.kubernetes.executor.podTemplateFile";
  private static final String SPARK_KUBERNETES_METRICS_PROPERTIES_CONF = "spark.metrics.conf";
  private static final String POD_TEMPLATE_FILE_NAME = "podTemplate-";
  private static final String CDAP_LOCALIZE_FILES_PATH = "/etc/cdap/localizefiles";
  private static final String CDAP_CONFIG_MAP_PREFIX = "cdap-compressed-files-";
  private static final String NAMESPACE_CREATION_ENABLED = "master.environment.k8s.namespace.creation.enabled";
  private static final String CDAP_NAMESPACE_LABEL = "cdap.namespace";
  private static final String RESOURCE_QUOTA_NAME = "cdap-resource-quota";

  private static final String DEFAULT_NAMESPACE = "default";
  private static final String DEFAULT_INSTANCE_LABEL = "cdap.instance";
  private static final String DEFAULT_CONTAINER_LABEL = "cdap.container";
  private static final String DEFAULT_POD_INFO_DIR = "/etc/podinfo";
  private static final String DEFAULT_POD_NAME_FILE = "pod.name";
  private static final String DEFAULT_POD_UID_FILE = "pod.uid";
  private static final String DEFAULT_POD_LABELS_FILE = "pod.labels.properties";
  private static final long DEFAULT_POD_KILLER_DELAY_MILLIS = TimeUnit.HOURS.toMillis(1L);

  private static final Pattern LABEL_PATTERN = Pattern.compile("(cdap\\..+?)=\"(.*)\"");

  private KubeDiscoveryService discoveryService;
  private PodKillerTask podKillerTask;
  private KubeTwillRunnerService twillRunner;
  private PodInfo podInfo;
  private Map<String, String> additionalSparkConfs;
  private File podInfoDir;
  private File podLabelsFile;
  private File podNameFile;
  private File podUidFile;
  // In memory state for holding configmap name. Used to delete this configmap upon master environment destroy.
  private String configMapName;
  private CoreV1Api coreV1Api;
  private boolean namespaceCreationEnabled;

  @Override
  public void initialize(MasterEnvironmentContext context) throws IOException, ApiException {
    LOG.info("Initializing Kubernetes environment");

    Map<String, String> conf = context.getConfigurations();
    podInfoDir = new File(conf.getOrDefault(POD_INFO_DIR, DEFAULT_POD_INFO_DIR));
    podLabelsFile = new File(podInfoDir, conf.getOrDefault(POD_LABELS_FILE, DEFAULT_POD_LABELS_FILE));
    podNameFile = new File(podInfoDir, conf.getOrDefault(POD_NAME_FILE, DEFAULT_POD_NAME_FILE));
    podUidFile = new File(podInfoDir, conf.getOrDefault(POD_UID_FILE, DEFAULT_POD_UID_FILE));
    namespaceCreationEnabled = Boolean.parseBoolean(conf.get(NAMESPACE_CREATION_ENABLED));

    // We don't support scaling from inside pod. Scaling should be done via CDAP operator.
    // Currently we don't support more than one instance per system service, hence set it to "1".
    conf.put(MASTER_MAX_INSTANCES, "1");
    // No TX in K8s
    conf.put(DATA_TX_ENABLED, Boolean.toString(false));

    // Load the pod labels from the configured path. It should be setup by the CDAP operator
    podInfo = createPodInfo(conf);
    Map<String, String> podLabels = podInfo.getLabels();

    String namespace = podInfo.getNamespace();
    additionalSparkConfs = getSparkConfigurations(conf);
    coreV1Api = new CoreV1Api(Config.defaultClient());

    // Get the instance label to setup prefix for K8s services
    String instanceLabel = conf.getOrDefault(INSTANCE_LABEL, DEFAULT_INSTANCE_LABEL);
    String instanceName = podLabels.get(instanceLabel);
    if (instanceName == null) {
      throw new IllegalStateException("Missing instance label '" + instanceLabel + "' from pod labels.");
    }

    // Services are publish to K8s with a prefix
    String resourcePrefix = "cdap-" + instanceName + "-";
    discoveryService = new KubeDiscoveryService(namespace, "cdap-" + instanceName + "-", podLabels,
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

    twillRunner = new KubeTwillRunnerService(context, namespace, discoveryService,
                                             podInfo, resourcePrefix,
                                             Collections.singletonMap(instanceLabel, instanceName),
                                             Integer.parseInt(conf.getOrDefault(JOB_CLEANUP_INTERVAL, "60")),
                                             Integer.parseInt(conf.getOrDefault(JOB_CLEANUP_BATCH_SIZE, "1000")));
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
    String master = getMasterPath();

    Map<String, String> sparkConfMap = new HashMap<>(additionalSparkConfs);
    // Create pod template with config maps and add to spark conf.
    sparkConfMap.put(SPARK_KUBERNETES_DRIVER_POD_TEMPLATE,
                     getDriverPodTemplate(podInfo, sparkSubmitContext).getAbsolutePath());
    sparkConfMap.put(SPARK_KUBERNETES_EXECUTOR_POD_TEMPLATE, getExecutorPodTemplateFile().getAbsolutePath());
    sparkConfMap.put(SPARK_KUBERNETES_METRICS_PROPERTIES_CONF, "/opt/spark/work-dir/metrics.properties");

    // Add spark pod labels. This will be same as job labels
    populateLabels(sparkConfMap);

    // Kube Master environment would always contain spark job jar file.
    // https://github.com/cdapio/cdap/blob/develop/cdap-spark-core3_2.12/src/k8s/Dockerfile#L46
    return new SparkConfig("k8s://" + master,
                           URI.create("local:/opt/cdap/cdap-spark-core/cdap-spark-core.jar"),
                           sparkConfMap);
  }

  @Override
  public void onNamespaceCreation(String cdapNamespace, Map<String, String> properties) throws Exception {
    // check if namespace creation is enabled from master config
    if (!namespaceCreationEnabled) {
      return;
    }
    String namespace = properties.get(NAMESPACE_PROPERTY);
    if (namespace == null || namespace.isEmpty()) {
      throw new IOException(String.format("Cannot create Kubernetes namespace for %s because no name was provided",
                                          cdapNamespace));
    }
    findOrCreateKubeNamespace(namespace, cdapNamespace);
    updateOrCreateResourceQuota(namespace, cdapNamespace, properties);
  }

  @Override
  public void onNamespaceDeletion(String cdapNamespace, Map<String, String> properties) throws Exception {
    String namespace = properties.get(NAMESPACE_PROPERTY);
    if (namespaceCreationEnabled && namespace != null && !namespace.isEmpty()) {
      deleteKubeNamespace(namespace, cdapNamespace);
    }
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
    String namespace = conf.getOrDefault(NAMESPACE_KEY, DEFAULT_NAMESPACE);

    if (!podInfoDir.isDirectory()) {
      throw new IllegalArgumentException(String.format("%s is not a directory.", podInfoDir.getAbsolutePath()));
    }

    // Load the pod labels from the configured path. It should be setup by the CDAP operator
    Map<String, String> podLabels = new HashMap<>();
    try (BufferedReader reader = Files.newBufferedReader(podLabelsFile.toPath(), StandardCharsets.UTF_8)) {
      String line = reader.readLine();
      while (line != null) {
        Matcher matcher = LABEL_PATTERN.matcher(line);
        if (matcher.matches()) {
          podLabels.put(matcher.group(1), matcher.group(2));
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
    CoreV1Api api = new CoreV1Api(Config.defaultClient());
    V1Pod pod = api.readNamespacedPod(podName, namespace, null, null, null);
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
                       podNameFile.getName(), podUid, podUidFile.getName(), namespace, podLabels, ownerReferences,
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

  private String getMasterPath() {
    // Spark master base path
    String master;
    // apiClient.getBasePath() returns path similar to https://10.8.0.1:443
    try {
      ApiClient apiClient = Config.defaultClient();
      master = apiClient.getBasePath();
    } catch (Exception e) {
      // should not happen
      throw new RuntimeException("Exception while getting kubernetes master path", e);
    }
    return master;
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

      // Add configmap as a volume to be added to the pod template
      driverPodSpec.addVolumesItem(new V1Volume().name(configMapName)
                                     .configMap(new V1ConfigMapVolumeSourceBuilder().withName(configMapName).build()));
      // Add configmap as a volume mount
      for (V1Container container : driverPodSpec.getContainers()) {
        container.addVolumeMountsItem(new V1VolumeMount().name(configMapName)
                                        .mountPath(CDAP_LOCALIZE_FILES_PATH).readOnly(true));
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

    // Create spark template file. We do not delete it because pod will get deleted at the end of job completion.
    return serializePodTemplate(executorPod);
  }

  private File serializePodTemplate(V1Pod v1Pod) throws IOException {
    File templateFile = new File(POD_TEMPLATE_FILE_NAME + UUID.randomUUID().toString());
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
    V1DownwardAPIVolumeSource podinfoVolume = new V1DownwardAPIVolumeSource()
      .defaultMode(420)
      .items(Arrays.asList(labelsFile, nameFile, uidFile));
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
      if (label.getKey().equals("cdap.container")) {
        // Make sure correct container name label is being added for driver and executor containers
        sparkConfMap.put(SPARK_KUBERNETES_DRIVER_LABEL_PREFIX + label.getKey(), "spark-kubernetes-driver");
        sparkConfMap.put(SPARK_KUBERNETES_EXECUTOR_LABEL_PREFIX + label.getKey(), "spark-kubernetes-executor");
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

  @VisibleForTesting
  void setCoreV1Api(CoreV1Api coreV1Api) {
    this.coreV1Api = coreV1Api;
  }

  @VisibleForTesting
  void setNamespaceCreationEnabled() {
    this.namespaceCreationEnabled = true;
  }
}
