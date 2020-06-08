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

import io.cdap.cdap.master.environment.k8s.PodInfo;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1LabelSelector;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1ObjectMetaBuilder;
import io.kubernetes.client.models.V1OwnerReference;
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
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillSpecification;
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
 * Kubernetes version of a TwillRunner.
 * A StatefulSet is created which runs program.
 */
public class StatefulSetTwillPreparer extends AbstractKubeTwillPreparer {
  private static final Logger LOG = LoggerFactory.getLogger(DeploymentTwillPreparer.class);
  private static final String PVC_NAME = "preview-runner-data";
  private static final String CONTAINER_NAME = "cdap-preview-container";

  private final PodInfo podInfo;

  StatefulSetTwillPreparer(ApiClient apiClient, String kubeNamespace, PodInfo podInfo, TwillSpecification spec,
                           RunId twillRunId, V1ObjectMeta resourceMeta, Map<String, String> cConf,
                           KubeTwillControllerFactory controllerFactory) {
    super(apiClient, kubeNamespace, podInfo, spec, twillRunId, resourceMeta, cConf, controllerFactory);
    this.podInfo = podInfo;
  }

  @Override
  protected void createKubeResources(V1ObjectMeta resourceMeta, V1ResourceRequirements resourceRequirements,
                                     List<V1EnvVar> envVars, long timeout,
                                     TimeUnit timeoutUnit) throws IOException, ApiException {
    AppsV1Api appsApi = new AppsV1Api(getApiClient());
    V1StatefulSet statefulSet = createStatefulSet(resourceMeta, resourceRequirements, envVars,
                                                  timeoutUnit.toMillis(timeout));
    LOG.info("Creating StatefulSet with spec to K8s {}", getApiClient().getJSON().serialize(statefulSet));
    try {
      appsApi.createNamespacedStatefulSet(getKubeNamespace(), statefulSet, "true", null, null);
    } catch (ApiException e) {
      if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
        throw e;
      }
    }
    LOG.info("Created StatefulSet with spec to K8s {}", getApiClient().getJSON().serialize(statefulSet));

    // Create headless service
    V1ObjectMeta serviceMeta = new V1ObjectMetaBuilder(resourceMeta).build();
    // For headleass service set owner reference as the preview-runner statefulset instead of preview manager
    V1OwnerReference ref = new V1OwnerReference();
    ref.setApiVersion(statefulSet.getApiVersion());
    ref.blockOwnerDeletion(true);
    ref.setKind(statefulSet.getKind());
    ref.setName(statefulSet.getMetadata().getName());
    ref.setController(true);
    ref.setUid(statefulSet.getMetadata().getUid());
    serviceMeta.setOwnerReferences(Collections.singletonList(ref));
    V1Service service = createService(serviceMeta);
    CoreV1Api coreV1Api = new CoreV1Api(getApiClient());
    LOG.info("Creating K8s service with spec {}", getApiClient().getJSON().serialize(service));
    try {
      coreV1Api.createNamespacedService(getKubeNamespace(), service, "true", null, null);
    } catch (ApiException e) {
      if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
        throw e;
      }
    }
    LOG.info("Created K8s service with spec {}", getApiClient().getJSON().serialize(service));
  }

  private V1StatefulSet createStatefulSet(V1ObjectMeta resourceMeta, V1ResourceRequirements resourceRequirements,
                                          List<V1EnvVar> envVars, long startTimeoutMillis) {
    V1StatefulSet statefulSet = new V1StatefulSet();

    // Copy the meta and add the container label
    V1ObjectMeta statefulSetMeta = new V1ObjectMetaBuilder(resourceMeta).build();
    statefulSetMeta.putLabelsItem(podInfo.getContainerLabelName(), CONTAINER_NAME);
    statefulSetMeta.putAnnotationsItem(KubeTwillRunnerService.START_TIMEOUT_ANNOTATION,
                                       Long.toString(startTimeoutMillis));
    statefulSet.setMetadata(statefulSetMeta);

    V1StatefulSetSpec statefulSetSpec = new V1StatefulSetSpec();
    V1LabelSelector labelSelector = new V1LabelSelector();
    labelSelector.matchLabels(statefulSetMeta.getLabels());
    statefulSetSpec.setSelector(labelSelector);
    // TODO: Hard coded for now
    statefulSetSpec.setReplicas(1);

    // We don't need to launch pods one after other since we do not have master-slave
    // configurations for the preview runner pods
    statefulSetSpec.setPodManagementPolicy("Parallel");


    V1PodTemplateSpec podTemplateSpec = new V1PodTemplateSpec();
    podTemplateSpec.setMetadata(statefulSetMeta);

    V1PodSpec podSpec = new V1PodSpec();

    // set termination grace period to 0 so pods can be deleted quickly
    podSpec.setTerminationGracePeriodSeconds(0L);

    // Define volumes in the pod
    V1Volume podInfoVolume = getPodInfoVolume();
    // Volume for conf
    List<V1Volume> volumes = new ArrayList<>(podInfo.getVolumes());
    volumes.add(podInfoVolume);
    podSpec.setVolumes(volumes);

    V1Container container = new V1Container();
    container.setName(CONTAINER_NAME);
    container.setImage(podInfo.getContainerImage());
    container.setImagePullPolicy("Always");

    container.addArgsItem("io.cdap.cdap.internal.app.runtime.k8s.PreviewRunnerMain");
    container.addArgsItem("--env=k8s");
    container.addArgsItem(String.format("--twillRunId=%s", getTwillRunId()));

    container.setEnv(envVars);

    container.setResources(resourceRequirements);

    // Add volume mounts to the container. Add those from the current pod for mount cdap and hadoop conf.
    container.setVolumeMounts(containerVolumeMounts());

    podSpec.addContainersItem(container);

    // For preview runner pods we can create another priority class with lower priority than
    // the sts-priority
    podSpec.setPriorityClassName("priority-medium");

    // podSpec.setInitContainers(createInitContainers());

    podTemplateSpec.setSpec(podSpec);
    statefulSetSpec.setTemplate(podTemplateSpec);

    statefulSetSpec.setVolumeClaimTemplates(Collections.singletonList(createPVC()));

    statefulSet.setSpec(statefulSetSpec);
    return statefulSet;
  }

  private V1PersistentVolumeClaim createPVC() {
    V1PersistentVolumeClaim pvc = new V1PersistentVolumeClaim();

    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(PVC_NAME);
    pvc.setMetadata(meta);

    V1PersistentVolumeClaimSpec spec = new V1PersistentVolumeClaimSpec();
    spec.setAccessModes(Collections.singletonList("ReadWriteOnce"));
    Map<String, Quantity> requests = new HashMap<>();
    // TODO: Hardcoded for now
    requests.put("storage", Quantity.fromString("10Gi"));
    V1ResourceRequirements resourceRequirements = new V1ResourceRequirements();
    resourceRequirements.setRequests(requests);
    spec.setResources(resourceRequirements);

    pvc.setSpec(spec);
    return pvc;
  }

  private List<V1VolumeMount> containerVolumeMounts() {
    List<V1VolumeMount> result = new ArrayList<>(podInfo.getContainerVolumeMounts());
    V1VolumeMount pvcMount = new V1VolumeMount();
    pvcMount.setReadOnly(false);
    pvcMount.setMountPath("/data");
    pvcMount.setName(PVC_NAME);
    result.add(pvcMount);
    return result;
  }

  private V1Service createService(V1ObjectMeta resourceMeta) {
    V1Service service = new V1Service();
    service.metadata(resourceMeta);
    // create the stateful set spec
    V1ServiceSpec serviceSpec = new V1ServiceSpec();
    serviceSpec.clusterIP("None");
    serviceSpec.selector(resourceMeta.getLabels());
    service.spec(serviceSpec);
    return service;
  }
}
