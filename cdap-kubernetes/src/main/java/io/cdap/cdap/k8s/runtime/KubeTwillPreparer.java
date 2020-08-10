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

import com.google.common.hash.Hashing;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.master.environment.k8s.PodInfo;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnable;
import io.cdap.cdap.master.spi.twill.StatefulDisk;
import io.cdap.cdap.master.spi.twill.StatefulTwillPreparer;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ContainerBuilder;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1DeploymentBuilder;
import io.kubernetes.client.models.V1DownwardAPIVolumeFile;
import io.kubernetes.client.models.V1DownwardAPIVolumeSource;
import io.kubernetes.client.models.V1EmptyDirVolumeSource;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1LabelSelector;
import io.kubernetes.client.models.V1ObjectFieldSelector;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1ObjectMetaBuilder;
import io.kubernetes.client.models.V1PersistentVolumeClaim;
import io.kubernetes.client.models.V1PersistentVolumeClaimBuilder;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodSpecBuilder;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.kubernetes.client.models.V1ResourceRequirementsBuilder;
import io.kubernetes.client.models.V1StatefulSet;
import io.kubernetes.client.models.V1StatefulSetBuilder;
import io.kubernetes.client.models.V1Volume;
import io.kubernetes.client.models.V1VolumeMount;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.Configs;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.DefaultLocalFile;
import org.apache.twill.internal.DefaultRuntimeSpecification;
import org.apache.twill.internal.DefaultTwillSpecification;
import org.apache.twill.internal.LogOnlyEventHandler;
import org.apache.twill.internal.TwillRuntimeSpecification;
import org.apache.twill.internal.json.TwillRuntimeSpecificationAdapter;
import org.apache.twill.internal.utils.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;


/**
 * Kubernetes version of a TwillRunner.
 * <p>
 * Runs a program in Kubernetes by creating a config-map of the app spec and program options resources that are
 * expected to be found in the local files of the RuntimeSpecification for the TwillRunnable.
 * A deployment is created that mounts the created config-map.
 * <p>
 * Most of these operations are no-ops as many of these methods and pretty closely coupled to the Hadoop implementation
 * and have no analogy in Kubernetes.
 */
class KubeTwillPreparer implements TwillPreparer, StatefulTwillPreparer<KubeTwillPreparer> {

  private static final Logger LOG = LoggerFactory.getLogger(KubeTwillPreparer.class);

  private static final String CPU_MULTIPLIER = "master.environment.k8s.container.cpu.multiplier";
  private static final String MEMORY_MULTIPLIER = "master.environment.k8s.container.memory.multiplier";
  private static final String DEFAULT_MULTIPLIER = "1.0";

  private final MasterEnvironmentContext masterEnvContext;
  private final ApiClient apiClient;
  private final String kubeNamespace;
  private final PodInfo podInfo;
  private final List<String> arguments;
  private final Map<String, List<String>> runnableArgs;
  private final Map<String, StatefulRunnable> statefulRunnables;
  private final List<URI> resources;
  private final Set<String> runnables;
  private final Map<String, Map<String, String>> environments;
  private final RunId twillRunId;
  private final Location appLocation;
  private final KubeTwillControllerFactory controllerFactory;
  private final TwillSpecification twillSpec;
  private final String resourcePrefix;
  private final Map<String, String> extraLabels;

  KubeTwillPreparer(MasterEnvironmentContext masterEnvContext, ApiClient apiClient, String kubeNamespace,
                    PodInfo podInfo, TwillSpecification spec, RunId twillRunId, Location appLocation,
                    String resourcePrefix, Map<String, String> extraLabels,
                    KubeTwillControllerFactory controllerFactory) {
    // only expect one runnable for now
    if (spec.getRunnables().size() != 1) {
      throw new IllegalStateException("Kubernetes runner currently only supports one Twill Runnable");
    }
    this.masterEnvContext = masterEnvContext;
    this.apiClient = apiClient;
    this.kubeNamespace = kubeNamespace;
    this.podInfo = podInfo;
    this.runnables = spec.getRunnables().keySet();
    this.arguments = new ArrayList<>();
    this.runnableArgs = runnables.stream().collect(Collectors.toMap(r -> r, r -> new ArrayList<>()));
    this.statefulRunnables = new HashMap<>();
    this.resources = new ArrayList<>();
    this.controllerFactory = controllerFactory;
    this.environments = runnables.stream().collect(Collectors.toMap(r -> r, r -> new HashMap<>()));
    this.twillRunId = twillRunId;
    this.appLocation = appLocation;
    this.twillSpec = spec;
    this.resourcePrefix = resourcePrefix;
    this.extraLabels = extraLabels;
  }

  @Override
  public KubeTwillPreparer withStatefulRunnable(String runnableName,
                                                boolean orderedStart, StatefulDisk... disks) {
    if (!twillSpec.getRunnables().containsKey(runnableName)) {
      throw new IllegalArgumentException("Runnable " + runnableName + " not found");
    }

    if (Arrays.stream(disks).map(StatefulDisk::getName).collect(Collectors.toSet()).size() != disks.length) {
      throw new IllegalArgumentException("Each stateful disk must have unique name");
    }
    if (Arrays.stream(disks).map(StatefulDisk::getMountPath).collect(Collectors.toSet()).size() != disks.length) {
      throw new IllegalArgumentException("Each stateful disk must have unique mount path");
    }
    statefulRunnables.put(runnableName, new StatefulRunnable(orderedStart, Arrays.asList(disks)));
    return this;
  }

  @Override
  public TwillPreparer withConfiguration(Map<String, String> config) {
    // no-op
    return this;
  }

  @Override
  public TwillPreparer withConfiguration(String runnableName, Map<String, String> config) {
    // no-op
    return this;
  }

  @Override
  public TwillPreparer addLogHandler(LogHandler handler) {
    // no-op
    return this;
  }

  @Override
  public TwillPreparer setUser(String user) {
    // no-op
    return this;
  }

  @Override
  public TwillPreparer setSchedulerQueue(String name) {
    // no-op
    return this;
  }

  @Override
  public TwillPreparer setJVMOptions(String options) {
    for (String runnable : runnables) {
      setJVMOptions(runnable, options);
    }
    return this;
  }

  @Override
  public TwillPreparer setJVMOptions(String runnableName, String options) {
    return this;
  }

  @Override
  public TwillPreparer addJVMOptions(String options) {
    return this;
  }

  @Override
  public TwillPreparer enableDebugging(String... runnables) {
    return enableDebugging(false, runnables);
  }

  @Override
  public TwillPreparer enableDebugging(boolean doSuspend, String... runnables) {
    return this;
  }

  @Override
  public TwillPreparer withApplicationArguments(String... args) {
    return withApplicationArguments(Arrays.asList(args));
  }

  @Override
  public TwillPreparer withApplicationArguments(Iterable<String> args) {
    args.forEach(arguments::add);
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, String... args) {
    return withArguments(runnableName, Arrays.asList(args));
  }

  @Override
  public TwillPreparer withArguments(String runnableName, Iterable<String> args) {
    List<String> runnableArgs = this.runnableArgs.get(runnableName);
    if (runnableArgs == null) {
      throw new IllegalArgumentException("Runnable " + runnableName + " not found");
    }
    args.forEach(runnableArgs::add);
    return this;
  }

  @Override
  public TwillPreparer withDependencies(Class<?>... classes) {
    return withDependencies(Arrays.asList(classes));
  }

  @Override
  public TwillPreparer withDependencies(Iterable<Class<?>> classes) {
    // no-op
    return this;
  }

  @Override
  public TwillPreparer withResources(URI... resources) {
    return withResources(Arrays.asList(resources));
  }

  @Override
  public TwillPreparer withResources(Iterable<URI> resources) {
    for (URI resource : resources) {
      this.resources.add(resource);
    }
    return this;
  }

  @Override
  public TwillPreparer withClassPaths(String... classPaths) {
    return withClassPaths(Arrays.asList(classPaths));
  }

  @Override
  public TwillPreparer withClassPaths(Iterable<String> classPaths) {
    return this;
  }

  @Override
  public TwillPreparer withEnv(Map<String, String> env) {
    for (String runnableName : runnables) {
      withEnv(runnableName, env);
    }
    return this;
  }

  @Override
  public TwillPreparer withEnv(String runnableName, Map<String, String> env) {
    Map<String, String> runnableEnv = environments.get(runnableName);
    if (runnableEnv == null) {
      throw new IllegalArgumentException("Runnable " + runnableName + " not found");
    }
    runnableEnv.putAll(env);
    return this;
  }

  @Override
  public TwillPreparer withApplicationClassPaths(String... classPaths) {
    return this;
  }

  @Override
  public TwillPreparer withApplicationClassPaths(Iterable<String> classPaths) {
    return this;
  }

  @Override
  public TwillPreparer withBundlerClassAcceptor(ClassAcceptor classAcceptor) {
    return this;
  }

  @Override
  public TwillPreparer withMaxRetries(String runnableName, int maxRetries) {
    return this;
  }

  @Override
  public TwillPreparer addSecureStore(SecureStore secureStore) {
    return this;
  }

  @Override
  public TwillPreparer setLogLevel(LogEntry.Level logLevel) {
    return setLogLevels(Collections.singletonMap(Logger.ROOT_LOGGER_NAME, logLevel));
  }

  @Override
  public TwillPreparer setLogLevels(Map<String, LogEntry.Level> logLevels) {
    return this;
  }

  @Override
  public TwillPreparer setLogLevels(String runnableName, Map<String, LogEntry.Level> logLevelsForRunnable) {
    return this;
  }

  @Override
  public TwillPreparer setClassLoader(String classLoaderClassName) {
    return this;
  }

  @Override
  public TwillController start() {
    return start(60, TimeUnit.SECONDS);
  }

  @Override
  public TwillController start(long timeout, TimeUnit timeoutUnit) {
    try {
      Path runtimeConfigDir = Files.createTempDirectory(Constants.Files.RUNTIME_CONFIG_JAR);
      Location runtimeConfigLocation;
      try {
        saveSpecification(twillSpec, runtimeConfigDir.resolve(Constants.Files.TWILL_SPEC));
        saveArguments(arguments, runnableArgs, runtimeConfigDir.resolve(Constants.Files.ARGUMENTS));
        runtimeConfigLocation = createRuntimeConfigJar(runtimeConfigDir);
      } finally {
        Paths.deleteRecursively(runtimeConfigDir);
      }

      RuntimeSpecification runtimeSpec = twillSpec.getRunnables().values().iterator().next();
      StatefulRunnable statefulRunnable = statefulRunnables.get(runtimeSpec.getName());
      Type resourceType = statefulRunnable == null ? V1Deployment.class : V1StatefulSet.class;

      V1ObjectMeta metadata = createResourceMetadata(resourceType, runtimeSpec.getName(),
                                                     timeoutUnit.toMillis(timeout));
      if (V1Deployment.class.equals(resourceType)) {
        metadata = createDeployment(metadata, runtimeSpec, runtimeConfigLocation);
      } else {
        metadata = createStatefulSet(metadata, runtimeSpec, runtimeConfigLocation, statefulRunnable);
      }

      return controllerFactory.create(resourceType, metadata, timeout, timeoutUnit);
    } catch (Exception e) {
      try {
        appLocation.delete(true);
      } catch (IOException ex) {
        e.addSuppressed(ex);
      }
      throw new RuntimeException("Unable to create Kubernetes resource while attempting to start program.", e);
    }
  }

  /**
   * Creates a {@link V1ObjectMeta} for the given resource type.
   */
  private V1ObjectMeta createResourceMetadata(Type resourceType, String runnableName, long startTimeoutMillis) {
    // For StatefulSet, it generates a label for each pod, with format of [statefulset_name]-[10_chars_hash],
    // hence the allowed resource name has to be <= 52
    String resourceName = getResourceName(twillSpec.getName(), twillRunId,
                                          V1Deployment.class.equals(resourceType) ? 240 : 52);

    Map<String, String> extraLabels = this.extraLabels.entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> asLabel(e.getValue())));

    // labels have more strict requirements around valid character sets,
    // so use annotations to store the app name.
    return new V1ObjectMetaBuilder()
      .withName(resourceName)
      .withOwnerReferences(podInfo.getOwnerReferences())
      .addToLabels(extraLabels)
      .addToLabels(podInfo.getContainerLabelName(), runnableName)
      .addToAnnotations(KubeTwillRunnerService.APP_LABEL, twillSpec.getName())
      .addToAnnotations(KubeTwillRunnerService.START_TIMEOUT_ANNOTATION, Long.toString(startTimeoutMillis))
      .build();
  }

  /**
   * Returns the name for the resource to be deployed in Kubernetes.
   */
  private String getResourceName(String appName, RunId runId, int maxLength) {
    String fullName = resourcePrefix + appName + "-" + runId;
    // Don't trim when cleansing the name
    fullName = cleanse(fullName, Integer.MAX_VALUE);

    if (fullName.length() <= maxLength) {
      return fullName;
    }

    // Generates a hash and takes the first 5 bytes of it. It should be unique enough in our use case.
    String hash = "-" + Hashing.sha256().hashString(fullName, StandardCharsets.UTF_8).toString().substring(0, 10);
    return fullName.substring(0, maxLength - hash.length()) + hash;
  }

  /**
   * label values must be 63 characters or less and consist of alphanumeric, '.', '_', or '-'.
   */
  private String asLabel(String val) {
    return cleanse(val, 63);
  }

  /**
   * Kubernetes names must be lower case alphanumeric, or '-'. Some are less restrictive, but those characters
   * should always be ok.
   */
  private String cleanse(String val, int maxLength) {
    String cleansed = val.replaceAll("[^A-Za-z0-9\\-_]", "-").toLowerCase();
    return cleansed.length() > maxLength ? cleansed.substring(0, maxLength) : cleansed;
  }

  /**
   * Deploys a {@link V1Deployment} to for runnable exeuction in Kubernetes.
   */
  private V1ObjectMeta createDeployment(V1ObjectMeta metadata,
                                        RuntimeSpecification runtimeSpec,
                                        Location runtimeConfigLocation) throws ApiException {
    AppsV1Api appsApi = new AppsV1Api(apiClient);

    V1Deployment deployment = buildDeployment(metadata, runtimeSpec, runtimeConfigLocation);

    deployment = appsApi.createNamespacedDeployment(kubeNamespace, deployment, "true", null, null);
    LOG.info("Created deployment {} in Kubernetes", metadata.getName());
    return deployment.getMetadata();
  }

  /**
   * Deploys a {@link V1StatefulSet} to for runnable exeuction in Kubernetes.
   */
  private V1ObjectMeta createStatefulSet(V1ObjectMeta metadata,
                                         RuntimeSpecification runtimeSpec,
                                         Location runtimeConfigLocation,
                                         StatefulRunnable statefulRunnable) throws ApiException {
    AppsV1Api appsApi = new AppsV1Api(apiClient);

    V1StatefulSet statefulSet = buildStatefulSet(metadata, runtimeSpec, runtimeConfigLocation, statefulRunnable);

    statefulSet = appsApi.createNamespacedStatefulSet(kubeNamespace, statefulSet, "true", null, null);
    LOG.info("Created deployment {} in Kubernetes", metadata.getName());
    return statefulSet.getMetadata();
  }

  /**
   * Return a {@link V1Deployment} object object for the {@link TwillRunnable} represented by the
   * given {@link RuntimeSpecification}
   */
  private V1Deployment buildDeployment(V1ObjectMeta metadata,
                                       RuntimeSpecification runtimeSpec, Location runtimeConfigLocation) {
    return new V1DeploymentBuilder()
      .withMetadata(metadata)
      .withNewSpec()
        .withSelector(new V1LabelSelector().matchLabels(metadata.getLabels()))
        .withReplicas(runtimeSpec.getResourceSpecification().getInstances())
        .withNewTemplate()
          .withMetadata(metadata)
          .withSpec(createPodSpec(runtimeConfigLocation, runtimeSpec))
        .endTemplate()
      .endSpec()
      .build();
  }

  /**
   * Returns a {@link V1StatefulSet} object for the {@link TwillRunnable} represented by the
   * given {@link RuntimeSpecification}
   */
  private V1StatefulSet buildStatefulSet(V1ObjectMeta metadata, RuntimeSpecification runtimeSpec,
                                         Location runtimeConfigLocation, StatefulRunnable statefulRunnable) {
    List<StatefulDisk> disks = statefulRunnable.getStatefulDisks();

    return new V1StatefulSetBuilder()
      .withMetadata(metadata)
      .withNewSpec()
        .withSelector(new V1LabelSelector().matchLabels(metadata.getLabels()))
        .withReplicas(runtimeSpec.getResourceSpecification().getInstances())
        .withPodManagementPolicy(statefulRunnable.isOrderedStart() ? "OrderedReady" : "Parallel")
        .addAllToVolumeClaimTemplates(disks.stream().map(this::createPVC).collect(Collectors.toList()))
        .withNewTemplate()
          .withMetadata(metadata)
          .withSpec(createPodSpec(runtimeConfigLocation, runtimeSpec,
                                  disks.stream().map(this::createDiskMount).toArray(V1VolumeMount[]::new)))
        .endTemplate()
      .endSpec()
      .build();
  }

  /**
   * Calculates the max heap size for a given total RAM size based on configurations
   */
  private int computeMaxHeapSize(V1ResourceRequirements resourceRequirements) {
    // Gets the memory from either the requests or the limits
    Quantity memory = Optional.ofNullable(resourceRequirements.getRequests())
      .map(m -> m.get("memory"))
      .orElse(Optional.ofNullable(resourceRequirements.getLimits()).map(m -> m.get("memory")).orElse(null));

    if (memory == null) {
      throw new IllegalArgumentException("No memory settings in the given resource requirements");
    }
    int memoryMB = (int) (memory.getNumber().longValue() >> 20);

    Map<String, String> cConf = masterEnvContext.getConfigurations();
    int reservedMemoryMB = Integer.parseInt(cConf.get(Configs.Keys.JAVA_RESERVED_MEMORY_MB));
    double minHeapRatio = Double.parseDouble(cConf.get(Configs.Keys.HEAP_RESERVED_MIN_RATIO));
    return org.apache.twill.internal.utils.Resources.computeMaxHeapSize(memoryMB, reservedMemoryMB, minHeapRatio);
  }

  /**
   * Creates and saves a {@link TwillRunnableSpecification} to a given path.
   */
  private void saveSpecification(TwillSpecification spec, Path targetFile) throws IOException {
    Map<String, Collection<LocalFile>> runnableLocalFiles = populateRunnableLocalFiles(spec);

    // Rewrite LocalFiles inside twillSpec
    Map<String, RuntimeSpecification> runtimeSpec = spec.getRunnables().entrySet().stream()
      .map(e -> new AbstractMap.SimpleImmutableEntry<>(
        e.getKey(), new DefaultRuntimeSpecification(e.getValue().getName(), e.getValue().getRunnableSpecification(),
                                                    e.getValue().getResourceSpecification(),
                                                    runnableLocalFiles.get(e.getKey()))))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    LOG.debug("Saving twill specification for {} to {}", spec.getName(), targetFile);
    TwillSpecification newTwillSpec = new DefaultTwillSpecification(spec.getName(), runtimeSpec, spec.getOrders(),
                                                                    spec.getPlacementPolicies(),
                                                                    new LogOnlyEventHandler().configure());
    TwillRuntimeSpecification twillRuntimeSpec = new TwillRuntimeSpecification(
      newTwillSpec, appLocation.getLocationFactory().getHomeLocation().getName(),
      appLocation.toURI(), "", twillRunId, twillSpec.getName(), "",
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());

    try (Writer writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_8)) {
      TwillRuntimeSpecificationAdapter.create().toJson(twillRuntimeSpec, writer);
    }
  }

  /**
   * Saves the application and runnable arguments to the given path.
   */
  private void saveArguments(List<String> appArgs,
                             Map<String, List<String>> runnableArgs, Path targetFile) throws IOException {
    LOG.debug("Save twill arguments to {}", targetFile);
    Gson gson = new Gson();
    JsonObject jsonObj = new JsonObject();
    jsonObj.add("arguments", gson.toJsonTree(appArgs));
    jsonObj.add("runnableArguments",
                gson.toJsonTree(runnableArgs, new TypeToken<Map<String, List<String>>>() { }.getType()));
    try (Writer writer = Files.newBufferedWriter(targetFile, StandardCharsets.UTF_8)) {
      gson.toJson(jsonObj, writer);
    }
  }

  /**
   * Creates a jar from the runtime config directory and upload it to a {@link Location}.
   */
  private Location createRuntimeConfigJar(Path dir) throws IOException {
    LOG.debug("Create and upload {}", dir);

    // Jar everything under the given directory, which contains different files needed by AM/runnable containers
    Location location = createTempLocation(Constants.Files.RUNTIME_CONFIG_JAR);
    try (
      JarOutputStream jarOutput = new JarOutputStream(location.getOutputStream());
      DirectoryStream<Path> stream = Files.newDirectoryStream(dir)
    ) {
      for (Path path : stream) {
        jarOutput.putNextEntry(new JarEntry(path.getFileName().toString()));
        Files.copy(path, jarOutput);
        jarOutput.closeEntry();
      }
    }

    return location;
  }

  /**
   * Based on the given {@link TwillSpecification}, upload LocalFiles to {@link Location}s.
   *
   * @param spec The {@link TwillSpecification} for populating resource.
   */
  private Map<String, Collection<LocalFile>> populateRunnableLocalFiles(TwillSpecification spec) throws IOException {
    Map<String, Collection<LocalFile>> localFiles = new HashMap<>();
    String locationScheme = appLocation.toURI().getScheme();

    for (Map.Entry<String, RuntimeSpecification> entry: spec.getRunnables().entrySet()) {
      String runnableName = entry.getKey();
      Collection<LocalFile> runnableFiles = localFiles.computeIfAbsent(runnableName, k -> new ArrayList<>());

      for (LocalFile localFile : entry.getValue().getLocalFiles()) {
        Location location;

        URI uri = localFile.getURI();
        if (locationScheme.equals(uri.getScheme())) {
          // If the source file location is having the same scheme as the target location, no need to copy
          location = appLocation.getLocationFactory().create(uri);
        } else {
          URL url = uri.toURL();
          LOG.debug("Create and copy {} : {}", runnableName, url);
          // Preserves original suffix for expansion.
          location = copyFromURL(url, createTempLocation(Paths.addExtension(url.getFile(), localFile.getName())));
          LOG.debug("Done {} : {}", runnableName, url);
        }

        runnableFiles.add(new DefaultLocalFile(localFile.getName(), location.toURI(), location.lastModified(),
                                               location.length(), localFile.isArchive(), localFile.getPattern()));
      }
    }

    return localFiles;
  }


  private Location copyFromURL(URL url, Location target) throws IOException {
    try (OutputStream os = new BufferedOutputStream(target.getOutputStream())) {
      Resources.copy(url, os);
      return target;
    }
  }

  private Location createTempLocation(String fileName) throws IOException {
    String suffix = Paths.getExtension(fileName);
    String name = fileName.substring(0, fileName.length() - suffix.length() - 1);
    return appLocation.append(name).getTempFile('.' + suffix);
  }

  /**
   * Creates a {@link V1PodSpec} for specifying pod information for running the given runnable.
   *
   * @param runtimeConfigLocation the {@link Location} containing the runtime config archive
   * @param runtimeSpec the specifiction for the {@link TwillRunnable} and its resources requirements
   * @return a {@link V1PodSpec}
   */
  private V1PodSpec createPodSpec(Location runtimeConfigLocation,
                                  RuntimeSpecification runtimeSpec, V1VolumeMount... extraMounts) {
    String runnableName = runtimeSpec.getName();
    String workDir = "/workDir-" + twillRunId.getId();

    V1Volume podInfoVolume = createPodInfoVolume(podInfo);
    V1ResourceRequirements resourceRequirements = createResourceRequirements(runtimeSpec.getResourceSpecification());

    // Add volume mounts to the container. Add those from the current pod for mount cdap and hadoop conf.
    List<V1VolumeMount> volumeMounts = new ArrayList<>(podInfo.getContainerVolumeMounts());
    volumeMounts.add(new V1VolumeMount().name(podInfoVolume.getName())
                       .mountPath(podInfo.getPodInfoDir()).readOnly(true));
    // Add the working directory the file localization by the init container
    volumeMounts.add(new V1VolumeMount().name("workdir").mountPath(workDir));
    volumeMounts.addAll(Arrays.asList(extraMounts));

    // Setup the container environment. Inherit everything from the current pod.
    Map<String, String> environs = podInfo.getContainerEnvironments().stream()
      .collect(Collectors.toMap(V1EnvVar::getName, V1EnvVar::getValue));
    // Add all environments for the runnable
    environs.putAll(environments.get(runnableName));

    return new V1PodSpecBuilder()
      .withServiceAccountName(podInfo.getServiceAccountName())
      .withRuntimeClassName(podInfo.getRuntimeClassName())
      .addAllToVolumes(podInfo.getVolumes())
      .addToVolumes(podInfoVolume,
                    new V1Volume().name("workdir").emptyDir(new V1EmptyDirVolumeSource()))
      .withInitContainers(createContainer("file-localizer", podInfo.getContainerImage(), workDir,
                                          resourceRequirements, volumeMounts, environs, FileLocalizer.class,
                                          runtimeConfigLocation.toURI().toString(), runnableName))
      .withContainers(createContainer(runnableName, podInfo.getContainerImage(), workDir,
                                      resourceRequirements, volumeMounts, environs, KubeTwillLauncher.class,
                                      runnableName))
      .build();
  }

  /**
   * Creates a {@link V1Container} specification for running a {@link MasterEnvironmentRunnable} in a container.
   */
  private V1Container createContainer(String name, String containerImage, String workDir,
                                      V1ResourceRequirements resourceRequirements,
                                      List<V1VolumeMount> volumeMounts,
                                      Map<String, String> environments,
                                      Class<? extends MasterEnvironmentRunnable> runnableClass, String... args) {
    Map<String, String> environs = new HashMap<>(environments);

    // Set the environments for controlling the working directory
    environs.put("CDAP_LOCAL_DIR", workDir);
    environs.put("CDAP_TEMP_DIR", "tmp");

    // Set the process memory is through the JAVA_HEAPMAX variable.
    environs.put("JAVA_HEAPMAX", String.format("-Xmx%dm", computeMaxHeapSize(resourceRequirements)));
    List<V1EnvVar> containerEnvironments = environs.entrySet().stream()
      .map(e -> new V1EnvVar().name(e.getKey()).value(e.getValue()))
      .collect(Collectors.toList());

    return new V1ContainerBuilder()
      .withName(cleanse(name, 254))
      .withImage(containerImage)
      .withWorkingDir(workDir)
      .withResources(resourceRequirements)
      .addAllToVolumeMounts(volumeMounts)
      .addAllToEnv(containerEnvironments)
      .addToArgs(masterEnvContext.getRunnableArguments(runnableClass, args))
      .build();
  }

  /**
   * Creates a {@link V1ResourceRequirements} based on the given {@link ResourceSpecification}.
   */
  private V1ResourceRequirements createResourceRequirements(ResourceSpecification resourceSpec) {
    Map<String, String> cConf = masterEnvContext.getConfigurations();
    float cpuMultiplier = Float.parseFloat(cConf.getOrDefault(CPU_MULTIPLIER, DEFAULT_MULTIPLIER));
    float memoryMultiplier = Float.parseFloat(cConf.getOrDefault(MEMORY_MULTIPLIER, DEFAULT_MULTIPLIER));
    int cpuToRequest = (int) (resourceSpec.getVirtualCores() * 1000 * cpuMultiplier);
    int memoryToRequest = (int) (resourceSpec.getMemorySize() * memoryMultiplier);

    return new V1ResourceRequirementsBuilder()
      .addToRequests("cpu", new Quantity(String.format("%dm", cpuToRequest)))
      .addToRequests("memory", new Quantity(String.format("%dMi", memoryToRequest)))
      .build();
  }

  /**
   * Creates a {@link V1Volume} for localizing pod information via downward API.
   */
  private V1Volume createPodInfoVolume(PodInfo podInfo) {
    return new V1Volume()
      .name("pod-info")
      .downwardAPI(
        new V1DownwardAPIVolumeSource()
          .addItemsItem(new V1DownwardAPIVolumeFile()
                          .fieldRef(new V1ObjectFieldSelector().fieldPath("metadata.name"))
                          .path(podInfo.getNameFile()))
          .addItemsItem(new V1DownwardAPIVolumeFile()
                          .fieldRef(new V1ObjectFieldSelector().fieldPath("metadata.labels"))
                          .path(podInfo.getLabelsFile())));
  }

  /**
   * Creates a {@link V1PersistentVolumeClaim} with the given disk information.
   *
   * @param disk the disk information
   * @return a {@link V1PersistentVolumeClaim}
   */
  private V1PersistentVolumeClaim createPVC(StatefulDisk disk) {
    return new V1PersistentVolumeClaimBuilder()
      .withMetadata(new V1ObjectMeta().name(cleanse(disk.getName(), 254)))
      .withNewSpec()
      .addToAccessModes("ReadWriteOnce")
      .withResources(
        new V1ResourceRequirements()
          .requests(Collections.singletonMap("storage",
                                             Quantity.fromString(String.format("%dGi", disk.getDiskSizeGB())))))
      .endSpec()
      .build();
  }

  /**
   * Creates a {@link V1VolumeMount} with the given disk information.
   */
  private V1VolumeMount createDiskMount(StatefulDisk disk) {
    return new V1VolumeMount().name(cleanse(disk.getName(), 254)).mountPath(disk.getMountPath());
  }

  /**
   * Class to hold information about stateful runnable.
   */
  private static final class StatefulRunnable {
    private final boolean orderedStart;
    private final List<StatefulDisk> statefulDisks;

    private StatefulRunnable(boolean orderedStart, List<StatefulDisk> statefulDisks) {
      this.orderedStart = orderedStart;
      this.statefulDisks = new ArrayList<>(statefulDisks);
    }

    boolean isOrderedStart() {
      return orderedStart;
    }

    List<StatefulDisk> getStatefulDisks() {
      return statefulDisks;
    }
  }
}
