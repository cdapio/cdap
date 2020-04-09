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
import io.cdap.cdap.master.environment.k8s.PodInfo;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1ConfigMap;
import io.kubernetes.client.models.V1ConfigMapVolumeSource;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1DeploymentSpec;
import io.kubernetes.client.models.V1DownwardAPIVolumeFile;
import io.kubernetes.client.models.V1DownwardAPIVolumeSource;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1LabelSelector;
import io.kubernetes.client.models.V1ObjectFieldSelector;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1ObjectMetaBuilder;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodTemplateSpec;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.kubernetes.client.models.V1Volume;
import io.kubernetes.client.models.V1VolumeMount;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.LocalFile;
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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
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
 * <p>
 * Runs a program in Kubernetes by creating a config-map of the app spec and program options resources that are
 * expected to be found in the local files of the RuntimeSpecification for the TwillRunnable.
 * A deployment is created that mounts the created config-map.
 * <p>
 * Most of these operations are no-ops as many of these methods and pretty closely coupled to the Hadoop implementation
 * and have no analogy in Kubernetes.
 */
class KubeTwillPreparer implements TwillPreparer {

  private static final Logger LOG = LoggerFactory.getLogger(KubeTwillPreparer.class);
  // Just use a static for the container inside pod that runs CDAP app.
  private static final String CONTAINER_NAME = "cdap-app-container";
  private static final String CPU_MULTIPLIER = "master.environment.k8s.container.cpu.multiplier";
  private static final String MEMORY_MULTIPLIER = "master.environment.k8s.container.memory.multiplier";
  private static final String DEFAULT_MULTIPLIER = "1.0";

  // These are equal to the constants used in DistributedProgramRunner
  private static final String APP_SPEC = "appSpec.json";
  private static final String PROGRAM_OPTIONS = "program.options.json";
  private final ApiClient apiClient;
  private final String kubeNamespace;
  private final PodInfo podInfo;
  private final List<URI> resources;
  private final Set<String> runnables;
  private final Map<String, Map<String, String>> environments;
  private final RunId twillRunId;
  private final V1ObjectMeta resourceMeta;
  private final Map<String, String> cConf;
  private final KubeTwillControllerFactory controllerFactory;
  private final URI appSpec;
  private final URI programOptions;
  private final int memoryMB;
  private final int vcores;

  KubeTwillPreparer(ApiClient apiClient, String kubeNamespace, PodInfo podInfo, TwillSpecification spec,
                    RunId twillRunId, V1ObjectMeta resourceMeta, Map<String, String> cConf,
                    KubeTwillControllerFactory controllerFactory) {
    // only expect one runnable for now
    if (spec.getRunnables().size() != 1) {
      throw new IllegalStateException("Kubernetes runner currently only supports one Twill Runnable");
    }
    this.apiClient = apiClient;
    this.kubeNamespace = kubeNamespace;
    this.podInfo = podInfo;
    this.runnables = spec.getRunnables().keySet();
    this.resources = new ArrayList<>();
    this.controllerFactory = controllerFactory;
    this.environments = runnables.stream().collect(Collectors.toMap(r -> r, r -> new HashMap<>()));
    this.twillRunId = twillRunId;
    this.resourceMeta = resourceMeta;
    this.cConf = cConf;
    RuntimeSpecification runtimeSpecification = spec.getRunnables().values().iterator().next();
    ResourceSpecification resourceSpecification = runtimeSpecification.getResourceSpecification();
    this.appSpec = runtimeSpecification.getLocalFiles().stream()
      .filter(f -> APP_SPEC.equals(f.getName()))
      .map(LocalFile::getURI)
      .findFirst()
      .orElseThrow(() -> new IllegalStateException("App Spec not found in files to localize."));
    this.programOptions = runtimeSpecification.getLocalFiles().stream()
      .filter(f -> PROGRAM_OPTIONS.equals(f.getName()))
      .map(LocalFile::getURI)
      .findFirst()
      .orElseThrow(() -> new IllegalStateException("Program Options not found in files to localize."));
    this.memoryMB = resourceSpecification.getMemorySize();
    this.vcores = resourceSpecification.getVirtualCores();
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
    // no-op
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, String... args) {
    return withArguments(runnableName, Arrays.asList(args));
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
      V1ConfigMap configMap = createConfigMap();
      createDeployment(configMap, timeoutUnit.toMillis(timeout));
      return controllerFactory.create(timeout, timeoutUnit);
    } catch (ApiException | IOException e) {
      throw new RuntimeException("Unable to create Kubernetes resource while attempting to start program.", e);
    }
  }

  /**
   * Return a {@link V1Deployment} object that will be deployed in Kubernetes to run the program
   */
  @VisibleForTesting
  V1Deployment buildDeployment(RunId runid,
                                      String appSpec,
                                      String programOptions,
                                      V1ObjectMeta deploymentMetadata,
                                      String containerName,
                                      int vCores,
                                      int memoryMB,
                                      Map<String, Map<String, String>> environments,
                                      V1ConfigMap configMap,
                                      PodInfo currentPodInfo,
                                      long startTimeoutMillis) {
    /*
       The deployment will look something like:
       apiVersion: apps/v1
       kind: Deployment
       metadata:
         [deployment meta]
       spec:
         [deployment spec]
       template:
         [template spec]
     */
    V1Deployment deployment = new V1Deployment();

    /*
       metadata:
         name: cdap-[instance name]-[cleansed twill app name]-runid
         labels:
           cdap.instance: cdap1
           cdap.container: cdap-app-container
           cdap.twill.runner: k8s
           cdap.twill.app: [cleansed twill app name]
           cdap.twill.run.id: [twill run id]
     */
    // Copy the meta and add the container label
    V1ObjectMeta deploymentMeta = new V1ObjectMetaBuilder(deploymentMetadata).build();
    deploymentMeta.putLabelsItem(currentPodInfo.getContainerLabelName(), containerName);
    deploymentMeta.putAnnotationsItem(KubeTwillRunnerService.START_TIMEOUT_ANNOTATION,
                                      Long.toString(startTimeoutMillis));
    deployment.setMetadata(deploymentMeta);

    /*
       spec:
         replicas: 1
         selector:
           matchLabels:
             cdap.instance: cdap1
             cdap.container: cdap-app-container
             cdap.twill.runner: k8s
             cdap.twill.app: [cleansed twill app name]
             cdap.twill.run.id: [twill run id]
     */
    V1DeploymentSpec spec = new V1DeploymentSpec();
    V1LabelSelector labelSelector = new V1LabelSelector();
    labelSelector.matchLabels(deploymentMeta.getLabels());
    spec.setSelector(labelSelector);
    // TODO: figure out where number of instances is specified in twill spec
    spec.setReplicas(1);

    /*
       template:
         metadata:
           labels:
             cdap.instance: cdap1
             cdap.container: cdap-app-container
             cdap.twill.runner: k8s
             cdap.twill.app: [cleansed twill app name]
             cdap.twill.run.id: [twill run id]
         spec:
           [pod spec]
     */
    V1PodTemplateSpec templateSpec = new V1PodTemplateSpec();
    templateSpec.setMetadata(deploymentMeta);

    /*
         spec:
           containers:
           - name: cdap-app-container
             image: gcr.io/cdap-dogfood/cdap-sandbox:k8s-26
             command: ["io.cdap.cdap.internal.app.runtime.k8s.UserServiceProgramMain"]
             args: ["--env=k8s",
                    "--appSpecPath=/etc/podinfo-app/appSpec", "--programOptions=/etc/podinfo-app/programOptions"]
             volumeMounts:
             - name: app-config
               mountPath: /etc/podinfo-app
             - name: pod-info
               mountPath: /etc/podinfo
           volumes:
           - name: app-config
             configMap:
               name: service-runid
           - name: pod-info
             downwardAPI:
               ...
     */
    V1PodSpec podSpec = new V1PodSpec();

    // Define volumes in the pod.
    V1Volume podInfoVolume = getPodInfoVolume();
    V1Volume configVolume = getConfigVolume(configMap);
    List<V1Volume> volumes = new ArrayList<>(currentPodInfo.getVolumes());
    volumes.add(podInfoVolume);
    volumes.add(configVolume);
    podSpec.setVolumes(volumes);

    // Set the service for RBAC control for app.
    podSpec.setServiceAccountName(currentPodInfo.getServiceAccountName());

    // Set the runtime class name to be the same as app-fabric
    // This is for protection against running user code
    podSpec.setRuntimeClassName(currentPodInfo.getRuntimeClassName());

    V1Container container = new V1Container();
    container.setName(containerName);
    container.setImage(currentPodInfo.getContainerImage());

    // Add volume mounts to the container. Add those from the current pod for mount cdap and hadoop conf.
    String configDir = currentPodInfo.getPodInfoDir() + "-app";
    List<V1VolumeMount> volumeMounts = new ArrayList<>(currentPodInfo.getContainerVolumeMounts());
    volumeMounts.add(new V1VolumeMount().name(podInfoVolume.getName())
                       .mountPath(currentPodInfo.getPodInfoDir()).readOnly(true));
    volumeMounts.add(new V1VolumeMount().name(configVolume.getName())
                       .mountPath(configDir).readOnly(true));
    container.setVolumeMounts(volumeMounts);

    V1ResourceRequirements resourceRequirements = new V1ResourceRequirements();
    Map<String, Quantity> quantityMap = new HashMap<>();
    float cpuMultiplier = Float.parseFloat(cConf.getOrDefault(CPU_MULTIPLIER, DEFAULT_MULTIPLIER));
    float memoryMultiplier = Float.parseFloat(cConf.getOrDefault(MEMORY_MULTIPLIER, DEFAULT_MULTIPLIER));
    quantityMap.put("cpu", new Quantity(String.format("%dm", (int) (vCores * 1000 * cpuMultiplier))));
    // Use slight larger container size
    quantityMap.put("memory", new Quantity(String.format("%dMi", (int) (memoryMB * memoryMultiplier * 1.2f))));
    resourceRequirements.setRequests(quantityMap);
    container.setResources(resourceRequirements);

    // Setup the container environment. Inherit everything from the current pod.
    Map<String, String> environs = currentPodInfo.getContainerEnvironments().stream()
      .collect(Collectors.toMap(V1EnvVar::getName, V1EnvVar::getValue));

    // assumption is there is only a single runnable
    environs.putAll(environments.values().iterator().next());

    // Set the process memory is through the JAVA_HEAPMAX variable.
    environs.put("JAVA_HEAPMAX", String.format("-Xmx%dm", memoryMB));
    container.setEnv(environs.entrySet().stream()
                       .map(e -> new V1EnvVar().name(e.getKey()).value(e.getValue()))
                       .collect(Collectors.toList()));

    // when other program types are supported, there will need to be a mapping from program type to main class
    container.setArgs(Arrays.asList("io.cdap.cdap.internal.app.runtime.k8s.UserServiceProgramMain", "--env=k8s",
                                    String.format("--appSpecPath=%s/%s", configDir, appSpec),
                                    String.format("--programOptions=%s/%s", configDir, programOptions),
                                    String.format("--twillRunId=%s", runid.getId())));
    podSpec.setContainers(Collections.singletonList(container));

    templateSpec.setSpec(podSpec);
    spec.setTemplate(templateSpec);
    deployment.setSpec(spec);

    return deployment;
  }

  /**
   * Build a {@link V1Deployment} object to run the program and deploy it in kubernete
   */
  private V1Deployment createDeployment(V1ConfigMap configMap, long startTimeoutMillis) throws ApiException {
    AppsV1Api appsApi = new AppsV1Api(apiClient);

    V1Deployment deployment = buildDeployment(twillRunId, APP_SPEC, PROGRAM_OPTIONS, resourceMeta, CONTAINER_NAME,
                                              vcores, memoryMB, environments, configMap, podInfo,
                                              startTimeoutMillis);

    deployment = appsApi.createNamespacedDeployment(kubeNamespace, deployment, "true", null, null);
    LOG.info("Created deployment {} in Kubernetes", resourceMeta.getName());
    return deployment;
  }

  /*
           volumes:
           - name: config-volume
             configMap:
               name: cdap-[instance name]-[twill app name]-[run-id]
   */
  private V1Volume getConfigVolume(V1ConfigMap configMap) {
    V1Volume volume = new V1Volume();
    volume.setName("app-config");

    V1ConfigMapVolumeSource configMapVolumeSource = new V1ConfigMapVolumeSource();
    configMapVolumeSource.setName(configMap.getMetadata().getName());
    volume.setConfigMap(configMapVolumeSource);
    return volume;
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
  private V1Volume getPodInfoVolume() {
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
   * Create the config map for the program run. Since the config map contains the program options, which contains
   * runtime arguments, it needs to be created for each program run and deleted when the run ends.
   */
  private V1ConfigMap createConfigMap() throws ApiException, IOException {
    CoreV1Api api = new CoreV1Api(apiClient);
    V1ConfigMap configMap = new V1ConfigMap();
    configMap.setMetadata(resourceMeta);
    configMap.putBinaryDataItem(APP_SPEC, Files.readAllBytes(new File(appSpec).toPath()));
    configMap.putBinaryDataItem(PROGRAM_OPTIONS, Files.readAllBytes(new File(programOptions).toPath()));
    LOG.trace("Creating config-map {} in Kubernetes", resourceMeta.getName());
    configMap = api.createNamespacedConfigMap(kubeNamespace, configMap, "true", null, null);
    LOG.trace("Created config-map {} in Kubernetes", resourceMeta.getName());
    return configMap;
  }
}
