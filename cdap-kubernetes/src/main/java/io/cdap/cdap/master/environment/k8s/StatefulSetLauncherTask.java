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

package io.cdap.cdap.master.environment.k8s;

import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentTask;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1LabelSelector;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1PersistentVolumeClaim;
import io.kubernetes.client.models.V1PersistentVolumeClaimSpec;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodTemplateSpec;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.models.V1ServiceSpec;
import io.kubernetes.client.models.V1StatefulSet;
import io.kubernetes.client.models.V1StatefulSetSpec;
import io.kubernetes.client.models.V1Volume;
import io.kubernetes.client.models.V1VolumeMount;
import io.kubernetes.client.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link MasterEnvironmentTask} for launching StatefulSet for preview runners.
 */
public class StatefulSetLauncherTask implements MasterEnvironmentTask {
  private static final Logger LOG = LoggerFactory.getLogger(StatefulSetLauncherTask.class);
  private static final String PREVIEW_RUNNER = "preview-runner";
  private static final String PVC_NAME = "preview-runner-data";
  private static final String CDAP = "cdap";
  private static final String DASH = "-";

  private final String baseName;
  private final String dockerImageName;
  private final String namespace;
  private final Map<String, String> labels;
  private final List<V1EnvVar> envVars;
  private final List<V1Volume> confVolumes;
  private final List<V1VolumeMount> containerConfVolumeMounts;
  private final List<V1Container> initContainers;
  private int numReplicas;
  private int delayMillis;
  private volatile CoreV1Api coreApi;
  private volatile AppsV1Api appsApi;

  /**
   * Constructor for the stateful set launcher task.
   * @param namespace the name of the Kubernetes namespace
   * @param dockerImageName the name of the CDAP docker image to be used by containers managed by this StatefulSet
   * @param numReplicas number of replicas for pod
   * @param instanceName name of the CDAP instance
   * @param delayMillis delay in milliseconds to wait before next retry for creating StatefulSet
   * @param envVars environment variables to be passed to the pods
   * @param confVolumes volumes associated with the ConfigMap to be attached to the pod
   * @param containerConfVolumeMounts volume mounts corresponding to the confVolumes for the container in the pod
   * @param initContainers init containers for the pod managed by StatefulSet
   */
  StatefulSetLauncherTask(String namespace, String dockerImageName, int numReplicas, String instanceName,
                          int delayMillis, List<V1EnvVar> envVars, List<V1Volume> confVolumes,
                          List<V1VolumeMount> containerConfVolumeMounts, List<V1Container> initContainers) {
    this.namespace = namespace;
    this.dockerImageName = dockerImageName;
    this.numReplicas = numReplicas;
    this.labels = new HashMap<>();
    this.baseName =  String.join(DASH, CDAP, instanceName, PREVIEW_RUNNER);
    labels.put("cdap.container", baseName);
    labels.put("cdap.instance", instanceName);
    this.envVars = Collections.unmodifiableList(new ArrayList<>(envVars));
    this.delayMillis = delayMillis;
    this.confVolumes = Collections.unmodifiableList(new ArrayList<>(confVolumes));
    this.containerConfVolumeMounts = Collections.unmodifiableList(new ArrayList<>(containerConfVolumeMounts));
    this.initContainers = Collections.unmodifiableList(new ArrayList<>(initContainers));
  }

  @Override
  public long run(MasterEnvironmentContext context) {
    CoreV1Api api;
    try {
      api = getCoreApi();
    } catch (IOException e) {
      LOG.warn("IO Exception raised when connecting to Kubernetes API server", e);
      return delayMillis;
    }

    AppsV1Api appsApi;
    try {
      appsApi = getAppsApi();
    } catch (IOException e) {
      LOG.warn("IO Exception raised when connecting to Kubernetes API server", e);
      return delayMillis;
    }

    V1StatefulSet statefulSet = createStatefulSet();
    LOG.info("Deploying spec to kb {}", coreApi.getApiClient().getJSON().serialize(statefulSet));

    try {
      appsApi.createNamespacedStatefulSet(namespace, statefulSet, null, null, null);
    } catch (ApiException e) {
      if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
        LOG.warn("API exception raised when trying to delete pods, code=" + e.getCode()
                   + ", body=" + e.getResponseBody(), e);
        return delayMillis;
      }
      LOG.info("StatefulSet already exists.");
    }

    V1Service service = createService();
    LOG.info("Deploying spec to kb {}", coreApi.getApiClient().getJSON().serialize(service));
    try {
      api.createNamespacedService(namespace, service, null, null, null);
    } catch (ApiException e) {
      // It means the service already exists.
      if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
        LOG.warn("API exception raised when trying to delete pods, code=" + e.getCode()
                   + ", body=" + e.getResponseBody(), e);
        return delayMillis;
      }
      LOG.info("Headless service already exists.");
    }
    return -1;
  }

  private V1StatefulSet createStatefulSet() {
    V1StatefulSet statefulSet = new V1StatefulSet();

    V1ObjectMeta objectMeta = new V1ObjectMeta();
    objectMeta.name(baseName);
    statefulSet.metadata(objectMeta);

    V1StatefulSetSpec statefulSetSpec = new V1StatefulSetSpec();
    statefulSetSpec.setServiceName(baseName);
    statefulSetSpec.setReplicas(numReplicas);

    // We don't need to launch pods one after other since we do not have master-slave
    // configurations for the preview runner pods
    statefulSetSpec.setPodManagementPolicy("Parallel");

    V1LabelSelector selector = new V1LabelSelector();
    selector.matchLabels(labels);
    statefulSetSpec.selector(selector);

    V1PodTemplateSpec podTemplateSpec = new V1PodTemplateSpec();
    V1ObjectMeta templateMetadata = new V1ObjectMeta().labels(labels);
    podTemplateSpec.setMetadata(templateMetadata);

    V1PodSpec podSpec = new V1PodSpec();

    // set termination grace period to 0 so pods can be deleted quickly
    podSpec.setTerminationGracePeriodSeconds(0L);

    V1Container container = new V1Container();
    container.setName(baseName);
    container.setImage(dockerImageName);
    container.setImagePullPolicy("Always");

    container.addArgsItem("io.cdap.cdap.master.environment.k8s.PreviewRunnerMain");
    container.addArgsItem("--env=k8s");

    container.setEnv(envVars);

    V1ResourceRequirements resourceRequirements = new V1ResourceRequirements();
    Map<String, Quantity> requests = new HashMap<>();
    requests.put("memory", Quantity.fromString("1500Mi"));
    requests.put("cpu", Quantity.fromString("200m"));
    resourceRequirements.setRequests(requests);
    resourceRequirements.setLimits(requests);
    container.setResources(resourceRequirements);

    container.setVolumeMounts(containerVolumeMounts());

    podSpec.addContainersItem(container);

    // For preview runner pods we can create another priority class with lower priority than
    // the sts-priority
    podSpec.setPriorityClassName("sts-priority");

    podSpec.setInitContainers(createInitContainers());

    podSpec.setVolumes(confVolumes);
    podTemplateSpec.setSpec(podSpec);
    statefulSetSpec.setTemplate(podTemplateSpec);

    statefulSetSpec.setVolumeClaimTemplates(Collections.singletonList(createPVC()));

    statefulSet.setSpec(statefulSetSpec);
    return statefulSet;
  }

  private V1Service createService() {
    V1Service service = new V1Service();

    // setup stateful set metadata
    V1ObjectMeta objectMeta = new V1ObjectMeta();
    objectMeta.name(baseName);
    service.metadata(objectMeta);

    // create the stateful set spec
    V1ServiceSpec serviceSpec = new V1ServiceSpec();

    serviceSpec.clusterIP("None");

    serviceSpec.selector(labels);
    service.spec(serviceSpec);
    return service;
  }

  private List<V1Container> createInitContainers() {
    List<V1Container> result = new ArrayList<>();
    for (V1Container initContainer : initContainers) {
      V1Container container = new V1Container();
      container.setArgs(initContainer.getArgs());
      container.setName(initContainer.getName());
      container.setImagePullPolicy(initContainer.getImagePullPolicy());
      container.setImage(initContainer.getImage());
      container.setResources(initContainer.getResources());
      container.setVolumeMounts(containerVolumeMounts());
      result.add(container);
    }
    return result;
  }

  private List<V1VolumeMount> containerVolumeMounts() {
    List<V1VolumeMount> result = new ArrayList<>(containerConfVolumeMounts);
    V1VolumeMount pvcMount = new V1VolumeMount();
    pvcMount.setReadOnly(false);
    pvcMount.setMountPath("/data");
    pvcMount.setName(PVC_NAME);
    result.add(pvcMount);
    return result;
  }

  private V1PersistentVolumeClaim createPVC() {
    V1PersistentVolumeClaim pvc = new V1PersistentVolumeClaim();

    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(PVC_NAME);
    pvc.setMetadata(meta);

    V1PersistentVolumeClaimSpec spec = new V1PersistentVolumeClaimSpec();
    spec.setAccessModes(Collections.singletonList("ReadWriteOnce"));
    Map<String, Quantity> requests = new HashMap<>();
    requests.put("storage", Quantity.fromString("10Gi"));
    V1ResourceRequirements resourceRequirements = new V1ResourceRequirements();
    resourceRequirements.setRequests(requests);
    spec.setResources(resourceRequirements);

    pvc.setSpec(spec);
    return pvc;
  }

  /**
   * Returns a {@link CoreV1Api} instance for interacting with the API server.
   *
   * @throws IOException if exception was raised during creation of {@link CoreV1Api}
   */
  private CoreV1Api getCoreApi() throws IOException {
    CoreV1Api api = coreApi;
    if (api != null) {
      return api;
    }

    synchronized (this) {
      api = coreApi;
      if (api != null) {
        return api;
      }

      ApiClient client = Config.defaultClient();

      // Set a reasonable timeout for the watch.
      client.getHttpClient().setReadTimeout(5, TimeUnit.MINUTES);

      coreApi = api = new CoreV1Api(client);
      return api;
    }
  }

  /**
   * Returns a {@link AppsV1Api} instance for interacting with the API server.
   *
   * @throws IOException if exception was raised during creation of {@link AppsV1Api}
   */
  private AppsV1Api getAppsApi() throws IOException {
    AppsV1Api api = appsApi;
    if (api != null) {
      return api;
    }

    synchronized (this) {
      api = appsApi;
      if (api != null) {
        return api;
      }

      ApiClient client = Config.defaultClient();

      // Set a reasonable timeout for the watch.
      client.getHttpClient().setReadTimeout(5, TimeUnit.MINUTES);

      appsApi = api = new AppsV1Api(client);
      return api;
    }
  }
}
