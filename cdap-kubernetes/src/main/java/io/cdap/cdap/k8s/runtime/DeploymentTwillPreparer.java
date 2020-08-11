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
import io.kubernetes.client.models.V1ConfigMap;
import io.kubernetes.client.models.V1ConfigMapVolumeSource;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1DeploymentSpec;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1LabelSelector;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1ObjectMetaBuilder;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodTemplateSpec;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.kubernetes.client.models.V1Volume;
import io.kubernetes.client.models.V1VolumeMount;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
class DeploymentTwillPreparer extends AbstractKubeTwillPreparer {

  private static final Logger LOG = LoggerFactory.getLogger(DeploymentTwillPreparer.class);
  // Just use a static for the container inside pod that runs CDAP app.
  private static final String CONTAINER_NAME = "cdap-app-container";

  // These are equal to the constants used in DistributedProgramRunner
  private static final String APP_SPEC = "appSpec.json";
  private static final String PROGRAM_OPTIONS = "program.options.json";
  private final PodInfo podInfo;
  private final URI appSpec;
  private final URI programOptions;

  DeploymentTwillPreparer(Map<String, String> cConf, ApiClient apiClient, String kubeNamespace, PodInfo podInfo,
                          TwillSpecification spec, RunId twillRunId, String resourcePrefix,
                          Map<String, String> extraLabels, KubeTwillControllerFactory controllerFactory) {
    super(cConf, apiClient, kubeNamespace, podInfo, spec, twillRunId, resourcePrefix, extraLabels, V1Deployment.class,
          controllerFactory);
    this.podInfo = podInfo;
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
  }

  @Override
  protected V1ObjectMeta createKubeResources(V1ObjectMeta resourceMeta, V1ResourceRequirements resourceRequirements,
                                             List<V1EnvVar> envVars, long timeout, TimeUnit timeoutUnit)
    throws IOException, ApiException {
    V1ConfigMap configMap = createConfigMap(resourceMeta);
    V1Deployment deployment = createDeployment(resourceMeta, resourceRequirements, envVars, configMap,
                                               timeoutUnit.toMillis(timeout));
    return deployment.getMetadata();
  }

  private V1Deployment createDeployment(V1ObjectMeta resourceMeta, V1ResourceRequirements resourceRequirements,
                                        List<V1EnvVar> envVars, V1ConfigMap configMap,
                                        long startTimeoutMillis) throws ApiException {
    AppsV1Api appsApi = new AppsV1Api(getApiClient());

    V1Deployment deployment = buildDeployment(resourceMeta, resourceRequirements, envVars, configMap,
                                              startTimeoutMillis);
    LOG.trace("Creating deployment {} in Kubernetes", resourceMeta.getName());
    deployment = appsApi.createNamespacedDeployment(getKubeNamespace(), deployment, "true", null, null);
    LOG.info("Created deployment {} in Kubernetes", resourceMeta.getName());
    return deployment;
  }

  /**
   * Return a {@link V1Deployment} object that will be deployed in Kubernetes to run the program
   */
  @VisibleForTesting
  V1Deployment buildDeployment(V1ObjectMeta resourceMeta, V1ResourceRequirements resourceRequirements,
                               List<V1EnvVar> envVars, V1ConfigMap configMap, long startTimeoutMillis) {

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
    V1ObjectMeta deploymentMeta = new V1ObjectMetaBuilder(resourceMeta).build();
    deploymentMeta.putLabelsItem(podInfo.getContainerLabelName(), CONTAINER_NAME);
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
    List<V1Volume> volumes = new ArrayList<>(podInfo.getVolumes());
    volumes.add(podInfoVolume);
    volumes.add(configVolume);
    podSpec.setVolumes(volumes);

    // Set the service for RBAC control for app.
    podSpec.setServiceAccountName(podInfo.getServiceAccountName());

    // Set the runtime class name to be the same as app-fabric
    // This is for protection against running user code
    podSpec.setRuntimeClassName(podInfo.getRuntimeClassName());

    V1Container container = new V1Container();
    container.setName(CONTAINER_NAME);
    container.setImage(podInfo.getContainerImage());

    // Add volume mounts to the container. Add those from the current pod for mount cdap and hadoop conf.
    String configDir = podInfo.getPodInfoDir() + "-app";
    List<V1VolumeMount> volumeMounts = new ArrayList<>(podInfo.getContainerVolumeMounts());
    volumeMounts.add(new V1VolumeMount().name(podInfoVolume.getName())
                       .mountPath(podInfo.getPodInfoDir()).readOnly(true));
    volumeMounts.add(new V1VolumeMount().name(configVolume.getName())
                       .mountPath(configDir).readOnly(true));
    container.setVolumeMounts(volumeMounts);

    container.setResources(resourceRequirements);

    container.setEnv(envVars);

    // when other program types are supported, there will need to be a mapping from program type to main class
    container.setArgs(Arrays.asList("io.cdap.cdap.internal.app.runtime.k8s.UserServiceProgramMain", "--env=k8s",
                                    String.format("--appSpecPath=%s/%s", configDir, APP_SPEC),
                                    String.format("--programOptions=%s/%s", configDir, PROGRAM_OPTIONS),
                                    String.format("--twillRunId=%s", getTwillRunId())));

    podSpec.setContainers(Collections.singletonList(container));

    templateSpec.setSpec(podSpec);
    spec.setTemplate(templateSpec);
    deployment.setSpec(spec);

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

  /**
   * Create the config map for the program run. Since the config map contains the program options, which contains
   * runtime arguments, it needs to be created for each program run and deleted when the run ends.
   */
  private V1ConfigMap createConfigMap(V1ObjectMeta resourceMeta) throws ApiException, IOException {
    CoreV1Api api = new CoreV1Api(getApiClient());
    V1ConfigMap configMap = new V1ConfigMap();
    configMap.setMetadata(resourceMeta);
    configMap.putBinaryDataItem(APP_SPEC, Files.readAllBytes(new File(appSpec).toPath()));
    configMap.putBinaryDataItem(PROGRAM_OPTIONS, Files.readAllBytes(new File(programOptions).toPath()));
    LOG.trace("Creating config-map {} in Kubernetes", resourceMeta.getName());
    configMap = api.createNamespacedConfigMap(getKubeNamespace(), configMap, "true", null, null);
    LOG.trace("Created config-map {} in Kubernetes", resourceMeta.getName());
    return configMap;
  }
}
