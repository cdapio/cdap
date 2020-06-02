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

import io.cdap.cdap.master.environment.k8s.PodInfo;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1DownwardAPIVolumeFile;
import io.kubernetes.client.models.V1DownwardAPIVolumeSource;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1ObjectFieldSelector;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1ResourceRequirements;
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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Kubernetes version of a TwillRunner.
 */
public abstract class AbstractKubeTwillPreparer implements TwillPreparer {

  private static final String CPU_MULTIPLIER = "master.environment.k8s.container.cpu.multiplier";
  private static final String MEMORY_MULTIPLIER = "master.environment.k8s.container.memory.multiplier";
  private static final String DEFAULT_MULTIPLIER = "1.0";

  private final ApiClient apiClient;
  private final String kubeNamespace;
  private final Set<String> runnables;
  private final KubeTwillControllerFactory controllerFactory;
  private final PodInfo podInfo;
  private final RunId twillRunId;
  private final List<URI> resources;
  private final V1ObjectMeta resourceMeta;
  private final Map<String, Map<String, String>> environments;
  private final int memoryMB;
  private final int vcores;
  private final Map<String, String> cConf;

  AbstractKubeTwillPreparer(ApiClient apiClient, String kubeNamespace, PodInfo podInfo,
                            TwillSpecification spec, RunId twillRunId, V1ObjectMeta resourceMeta,
                            Map<String, String> cConf, KubeTwillControllerFactory controllerFactory) {
    // only expect one runnable for now
    if (spec.getRunnables().size() != 1) {
      throw new IllegalStateException("Kubernetes runner currently only supports one Twill Runnable");
    }

    this.apiClient = apiClient;
    this.kubeNamespace = kubeNamespace;
    this.runnables = spec.getRunnables().keySet();
    this.controllerFactory = controllerFactory;
    this.podInfo = podInfo;
    this.environments = runnables.stream().collect(Collectors.toMap(r -> r, r -> new HashMap<>()));
    this.twillRunId = twillRunId;
    this.resources = new ArrayList<>();
    this.resourceMeta = resourceMeta;
    RuntimeSpecification runtimeSpecification = spec.getRunnables().values().iterator().next();
    ResourceSpecification resourceSpecification = runtimeSpecification.getResourceSpecification();
    this.memoryMB = resourceSpecification.getMemorySize();
    this.vcores = resourceSpecification.getVirtualCores();
    this.cConf = cConf;
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
      V1ResourceRequirements resourceRequirements = new V1ResourceRequirements();
      Map<String, Quantity> quantityMap = new HashMap<>();
      float cpuMultiplier = Float.parseFloat(cConf.getOrDefault(CPU_MULTIPLIER, DEFAULT_MULTIPLIER));
      float memoryMultiplier = Float.parseFloat(cConf.getOrDefault(MEMORY_MULTIPLIER, DEFAULT_MULTIPLIER));
      int cpuToRequest = (int) (vcores * 1000 * cpuMultiplier);
      int memoryToRequest = (int) (memoryMB * memoryMultiplier);
      quantityMap.put("cpu", new Quantity(String.format("%dm", cpuToRequest)));
      quantityMap.put("memory", new Quantity(String.format("%dMi", memoryToRequest)));
      resourceRequirements.setRequests(quantityMap);

      // Setup the container environment. Inherit everything from the current pod.
      Map<String, String> environs = podInfo.getContainerEnvironments().stream()
        .collect(Collectors.toMap(V1EnvVar::getName, V1EnvVar::getValue));

      // assumption is there is only a single runnable
      environs.putAll(environments.values().iterator().next());

      // Set the process memory is through the JAVA_HEAPMAX variable.
      environs.put("JAVA_HEAPMAX", String.format("-Xmx%dm", computeMaxHeapSize(memoryToRequest)));
      List<V1EnvVar> envVars = environs.entrySet().stream()
        .map(e -> new V1EnvVar().name(e.getKey()).value(e.getValue()))
        .collect(Collectors.toList());

      createKubeResources(resourceMeta, resourceRequirements, envVars, timeout, timeoutUnit);
      return controllerFactory.create(timeout, timeoutUnit);
    } catch (ApiException | IOException e) {
      throw new RuntimeException("Unable to create Kubernetes resource while attempting to start program.", e);
    }
  }

  /**
   * Calculates the max heap size for a given total RAM size based on configurations
   */
  private int computeMaxHeapSize(int memoryMB) {
    int reservedMemoryMB = Integer.parseInt(cConf.get(Configs.Keys.JAVA_RESERVED_MEMORY_MB));
    double minHeapRatio = Double.parseDouble(cConf.get(Configs.Keys.HEAP_RESERVED_MIN_RATIO));
    return org.apache.twill.internal.utils.Resources.computeMaxHeapSize(memoryMB, reservedMemoryMB, minHeapRatio);
  }

  protected abstract void createKubeResources(V1ObjectMeta resourceMeta, V1ResourceRequirements resourceRequirements,
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
}
