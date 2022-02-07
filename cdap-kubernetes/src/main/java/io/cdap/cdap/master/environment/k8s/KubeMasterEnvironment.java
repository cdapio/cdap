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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.k8s.common.AbstractWatcherThread;
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
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1OwnerReferenceBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodSpecBuilder;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watchable;
import io.kubernetes.client.util.Yaml;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.common.Cancellable;
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
import java.lang.reflect.Type;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nullable;

/**
 * Implementation of {@link MasterEnvironment} to provide the environment for running in Kubernetes.
 */
public class KubeMasterEnvironment implements MasterEnvironment {
  public static final String DISABLE_POD_DELETION = "disablePodDeletion";
  private static final Logger LOG = LoggerFactory.getLogger(KubeMasterEnvironment.class);

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
  private Cancellable podWatcherCancellable;

  @Override
  public void initialize(MasterEnvironmentContext context) throws IOException, ApiException {
    LOG.info("Initializing Kubernetes environment");

    Map<String, String> conf = context.getConfigurations();
    podInfoDir = new File(conf.getOrDefault(POD_INFO_DIR, DEFAULT_POD_INFO_DIR));
    podLabelsFile = new File(podInfoDir, conf.getOrDefault(POD_LABELS_FILE, DEFAULT_POD_LABELS_FILE));
    podNameFile = new File(podInfoDir, conf.getOrDefault(POD_NAME_FILE, DEFAULT_POD_NAME_FILE));
    podUidFile = new File(podInfoDir, conf.getOrDefault(POD_UID_FILE, DEFAULT_POD_UID_FILE));

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
    if (podWatcherCancellable != null) {
      podWatcherCancellable.cancel();
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

    CompletableFuture<Boolean> podStatusFuture = new CompletableFuture<>();
    this.podWatcherCancellable = watchForPodStatus(podStatusFuture);

    // Kube Master environment would always contain spark job jar file.
    // https://github.com/cdapio/cdap/blob/develop/cdap-spark-core3_2.12/src/k8s/Dockerfile#L46
    return new SparkConfig("k8s://" + master, URI.create("local:/opt/cdap/cdap-spark-core/cdap-spark-core.jar"),
                           sparkConfMap, podStatusFuture);
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

  private Cancellable watchForPodStatus(CompletableFuture<Boolean> podStatusFuture) {
    // Start watch for driver pod. This is added because of bug in spark implementation for driver pod status.
    // Check CDAP-18511 for details.
    Map<String, String> labels = podInfo.getLabels();
    // Spark label added by kubernetes
    labels.put("spark-role", "driver");
    String labelSelector = labels.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue())
      .collect(Collectors.joining(","));

    PodWatcherThread watcherThread = new PodWatcherThread(podInfo.getNamespace(), podStatusFuture,
                                                          coreV1Api, labelSelector);

    watcherThread.start();
    watcherThread.setDaemon(true);
    watcherThread.start();

    return watcherThread::close;
  }

  private static class PodWatcherThread extends AbstractWatcherThread<V1Pod> {
    private final CompletableFuture<Boolean> podStatusFuture;
    private final CoreV1Api coreV1Api;
    private final String labelSelector;

    PodWatcherThread(String namespace, CompletableFuture<Boolean> podStatusFuture,
                     CoreV1Api coreV1Api, String labelSelector) {
      super("kube-pod-watcher", namespace);
      this.podStatusFuture = podStatusFuture;
      this.coreV1Api = coreV1Api;
      this.labelSelector = labelSelector;
    }

    @Override
    public void resourceModified(V1Pod resource) {
      if (resource.getStatus() != null && resource.getStatus().getPhase() != null) {
        if (resource.getStatus().getPhase().equalsIgnoreCase("Succeeded")) {
          podStatusFuture.complete(true);
        } else if (resource.getStatus().getPhase().equalsIgnoreCase("Failed") ||
          resource.getStatus().getPhase().equalsIgnoreCase("Unknown")) {
          podStatusFuture.complete(false);
        }
      }
    }

    @Override
    public void resourceDeleted(V1Pod resource) {
      podStatusFuture.cancel(true);
    }

    @Nullable
    @Override
    protected String getSelector() {
      return labelSelector;
    }

    @Override
    protected Watchable<V1Pod> createWatchable(Type resourceType, String namespace,
                                               @Nullable String labelSelector) throws ApiException {
      return
        Watch.createWatch(
          coreV1Api.getApiClient(),
          coreV1Api.listNamespacedPodCall(
            namespace, null, null, null, null, labelSelector, 1, null, null, null, true, null),
          TypeToken.getParameterized(Watch.Response.class, resourceType).getType());
    }
  }
}
