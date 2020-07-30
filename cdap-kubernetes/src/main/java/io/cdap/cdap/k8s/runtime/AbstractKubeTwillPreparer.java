/*
 * Copyright © 2020 Cask Data, Inc.
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
import io.cdap.cdap.master.environment.k8s.PodInfo;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1DownwardAPIVolumeFile;
import io.kubernetes.client.models.V1DownwardAPIVolumeSource;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1ObjectFieldSelector;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1ObjectMetaBuilder;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.kubernetes.client.models.V1ResourceRequirementsBuilder;
import io.kubernetes.client.models.V1Volume;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.Configs;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Kubernetes version of a TwillRunner.
 */
public abstract class AbstractKubeTwillPreparer implements TwillPreparer {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractKubeTwillPreparer.class);
  private static final String CPU_MULTIPLIER = "master.environment.k8s.container.cpu.multiplier";
  private static final String MEMORY_MULTIPLIER = "master.environment.k8s.container.memory.multiplier";
  private static final String DEFAULT_MULTIPLIER = "1.0";

  private final Map<String, String> cConf;
  private final ApiClient apiClient;
  private final String kubeNamespace;
  private final PodInfo podInfo;
  private final TwillSpecification twillSpec;
  private final RunId twillRunId;
  private final String resourcePrefix;
  private final Map<String, String> extraLabels;
  private final Type resourceType;
  private final KubeTwillControllerFactory controllerFactory;
  private final List<URI> resources;
  private final Set<String> runnables;
  private final Map<String, Map<String, String>> environments;

  AbstractKubeTwillPreparer(Map<String, String> cConf, ApiClient apiClient, String kubeNamespace,
                            PodInfo podInfo, TwillSpecification spec, RunId twillRunId, String resourcePrefix,
                            Map<String, String> extraLabels, Type resourceType,
                            KubeTwillControllerFactory controllerFactory) {
    // only expect one runnable for now
    if (spec.getRunnables().size() != 1) {
      throw new IllegalStateException("Kubernetes runner currently only supports one Twill Runnable");
    }
    this.cConf = cConf;
    this.apiClient = apiClient;
    this.kubeNamespace = kubeNamespace;
    this.podInfo = podInfo;
    this.twillSpec = spec;
    this.twillRunId = twillRunId;
    this.resourcePrefix = resourcePrefix;
    this.extraLabels = extraLabels;
    this.resourceType = resourceType;
    this.controllerFactory = controllerFactory;
    this.runnables = spec.getRunnables().keySet();
    this.environments = runnables.stream().collect(Collectors.toMap(r -> r, r -> new HashMap<>()));
    this.resources = new ArrayList<>();
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
    // no-op
    return this;
  }

  @Override
  public TwillPreparer addJVMOptions(String options) {
    // no-op
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
    // no-op
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, String... args) {
    // no-op
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, Iterable<String> args) {
    // no-op
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
    environments.put(runnableName, env);
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
      RuntimeSpecification runtimeSpecification = twillSpec.getRunnables().values().iterator().next();
      V1ResourceRequirements resourceRequirements
        = createResourceRequirements(runtimeSpecification.getResourceSpecification());

      V1ObjectMeta resourceMeta = createResourceMetadata(resourceType, runtimeSpecification.getName(),
                                                         timeoutUnit.toMillis(timeout));

      List<V1EnvVar> envVars = createContainerEnvironments(resourceRequirements);

      resourceMeta = createKubeResources(resourceMeta, resourceRequirements, envVars, timeout, timeoutUnit);
      return controllerFactory.create(resourceType, resourceMeta, timeout, timeoutUnit);
    } catch (ApiException | IOException e) {
      if (e instanceof ApiException) {
        String msg = ((ApiException) e).getResponseBody();
        LOG.error("Error while creating object {}", msg);
      }
      throw new RuntimeException("Unable to create Kubernetes resource while attempting to start program.", e);
    }
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

    int reservedMemoryMB = Integer.parseInt(cConf.get(Configs.Keys.JAVA_RESERVED_MEMORY_MB));
    double minHeapRatio = Double.parseDouble(cConf.get(Configs.Keys.HEAP_RESERVED_MIN_RATIO));
    return org.apache.twill.internal.utils.Resources.computeMaxHeapSize(memoryMB, reservedMemoryMB, minHeapRatio);
  }

  protected abstract V1ObjectMeta createKubeResources(V1ObjectMeta resourceMeta,
                                                      V1ResourceRequirements resourceRequirements,
                                                      List<V1EnvVar> envVars, long timeout,
                                                      TimeUnit timeoutUnit) throws IOException, ApiException;

  ApiClient getApiClient() {
    return apiClient;
  }

  String getKubeNamespace() {
    return kubeNamespace;
  }

  String getTwillRunId() {
    return twillRunId.getId();
  }

  /*
     volumes:
       - name: pod-info
         downwardAPI:
           items:
             - path: "pod.labels.properties"
               fieldRef:
                 fieldPath: metadata.labels
             - path: "pod.name"
               fieldRef:
                 fieldPath: metadata.name
   */
  V1Volume getPodInfoVolume() {
    V1Volume volume = new V1Volume();
    volume.setName("pod-info");

    V1DownwardAPIVolumeSource downwardAPIVolumeSource = new V1DownwardAPIVolumeSource();

    V1DownwardAPIVolumeFile podNameFile = new V1DownwardAPIVolumeFile();
    V1ObjectFieldSelector nameRef = new V1ObjectFieldSelector();
    nameRef.setFieldPath("metadata.name");
    podNameFile.setFieldRef(nameRef);
    podNameFile.setPath(podInfo.getNameFile());

    V1DownwardAPIVolumeFile labelsFile = new V1DownwardAPIVolumeFile();
    V1ObjectFieldSelector labelsRef = new V1ObjectFieldSelector();
    labelsRef.setFieldPath("metadata.labels");
    labelsFile.setFieldRef(labelsRef);
    labelsFile.setPath(podInfo.getLabelsFile());

    downwardAPIVolumeSource.addItemsItem(podNameFile);
    downwardAPIVolumeSource.addItemsItem(labelsFile);

    volume.setDownwardAPI(downwardAPIVolumeSource);
    return volume;
  }

  /**
   * Creates a {@link V1ResourceRequirements} based on the given {@link ResourceSpecification}.
   */
  private V1ResourceRequirements createResourceRequirements(ResourceSpecification resourceSpec) {
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
   * Create container environment inherited from the current pod.
   */
  private List<V1EnvVar> createContainerEnvironments(V1ResourceRequirements resourceRequirements) {
    // Setup the container environment. Inherit everything from the current pod.
    Map<String, String> environs = podInfo.getContainerEnvironments().stream()
      .collect(Collectors.toMap(V1EnvVar::getName, V1EnvVar::getValue));

    // assumption is there is only a single runnable
    environs.putAll(environments.values().iterator().next());

    // Set the process memory is through the JAVA_HEAPMAX variable.
    environs.put("JAVA_HEAPMAX", String.format("-Xmx%dm", computeMaxHeapSize(resourceRequirements)));
    return environs.entrySet().stream()
      .map(e -> new V1EnvVar().name(e.getKey()).value(e.getValue()))
      .collect(Collectors.toList());
  }
}
