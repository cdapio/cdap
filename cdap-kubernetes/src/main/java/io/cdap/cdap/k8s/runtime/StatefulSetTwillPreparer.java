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
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1DownwardAPIVolumeFile;
import io.kubernetes.client.models.V1DownwardAPIVolumeSource;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1LabelSelector;
import io.kubernetes.client.models.V1ObjectFieldSelector;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1ObjectMetaBuilder;
import io.kubernetes.client.models.V1PersistentVolumeClaim;
import io.kubernetes.client.models.V1PersistentVolumeClaimSpec;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodTemplateSpec;
import io.kubernetes.client.models.V1ResourceRequirements;
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
  private static final String PREVIEW_RUNNERS_COUNT = "preview.runners.count";
  private static final String PREVIEW_RUNNER_TERMINATION_GRACE_PERIOD_SECONDS
    = "preview.runner.termination.grace.period.seconds";
  private static final String PREVIEW_RUNNER_PRIORITY_CLASS_NAME = "preview.runner.priority.class.name";

  private final PodInfo podInfo;
  private final Map<String, String> cConf;

  StatefulSetTwillPreparer(ApiClient apiClient, String kubeNamespace, PodInfo podInfo, TwillSpecification spec,
                           RunId twillRunId, V1ObjectMeta resourceMeta, Map<String, String> cConf,
                           KubeTwillControllerFactory controllerFactory) {
    super(apiClient, kubeNamespace, podInfo, spec, twillRunId, resourceMeta, cConf, controllerFactory);
    this.podInfo = podInfo;
    this.cConf = cConf;
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
    statefulSetSpec.setReplicas(Integer.parseInt(cConf.getOrDefault(PREVIEW_RUNNERS_COUNT, "1")));

    // We don't need to launch pods one after other since we do not have primary-secondary
    // configurations for the preview runner pods
    statefulSetSpec.setPodManagementPolicy("Parallel");

    V1PodTemplateSpec podTemplateSpec = new V1PodTemplateSpec();
    podTemplateSpec.setMetadata(statefulSetMeta);

    V1PodSpec podSpec = new V1PodSpec();

    podSpec.setTerminationGracePeriodSeconds(
      Long.parseLong(cConf.getOrDefault(PREVIEW_RUNNER_TERMINATION_GRACE_PERIOD_SECONDS, "5")));

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
    container.addArgsItem(String.format("--instanceNameFilePath=%s/%s", podInfo.getPodInfoDir(), "pod.name"));
    container.addArgsItem(String.format("--instanceUidFilePath=%s/%s", podInfo.getPodInfoDir(), "pod.uid"));

    container.setEnv(envVars);

    container.setResources(resourceRequirements);

    // Add volume mounts to the container.
    // Add those from the current pod for mount cdap and hadoop conf.
    List<V1VolumeMount> volumeMounts = new ArrayList<>(podInfo.getContainerVolumeMounts());
    // Add data volume mount
    volumeMounts.add(containerDataVolumeMount());
    // Add pod info
    volumeMounts.add(new V1VolumeMount().name(podInfoVolume.getName())
                       .mountPath(podInfo.getPodInfoDir()).readOnly(true));
    container.setVolumeMounts(volumeMounts);

    podSpec.addContainersItem(container);

    // For preview runner pods we can create another priority class with lower priority than
    // the sts-priority
    podSpec.setPriorityClassName(cConf.getOrDefault(PREVIEW_RUNNER_PRIORITY_CLASS_NAME, "priority-low"));

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

  private V1VolumeMount containerDataVolumeMount() {
    V1VolumeMount pvcMount = new V1VolumeMount();
    pvcMount.setReadOnly(false);
    pvcMount.setMountPath("/data");
    pvcMount.setName(PVC_NAME);
    return pvcMount;
  }

  /*
    volumes:
      - name: podinfo
        downwardAPI:
          items:
            - path: "pod.labels.properties"
              fieldRef:
                fieldPath: metadata.labels
            - path: "pod.name"
              fieldRef:
                fieldPath: metadata.name
            - path: "pod.uid"
              fieldRef:
                fieldPath: metadata.id
   */
   V1Volume getPodInfoVolume() {
     V1Volume volume = new V1Volume();
     volume.setName("podinfo");

     V1DownwardAPIVolumeSource downwardAPIVolumeSource = new V1DownwardAPIVolumeSource();

     V1DownwardAPIVolumeFile podNameFile = new V1DownwardAPIVolumeFile();
     V1ObjectFieldSelector nameRef = new V1ObjectFieldSelector();
     nameRef.setFieldPath("metadata.name");
     podNameFile.setFieldRef(nameRef);
     podNameFile.setPath("pod.name");

     V1DownwardAPIVolumeFile labelsFile = new V1DownwardAPIVolumeFile();
     V1ObjectFieldSelector labelsRef = new V1ObjectFieldSelector();
     labelsRef.setFieldPath("metadata.labels");
     labelsFile.setFieldRef(labelsRef);
     labelsFile.setPath("pod.labels.properties");

     V1DownwardAPIVolumeFile idFile = new V1DownwardAPIVolumeFile();
     V1ObjectFieldSelector idRef = new V1ObjectFieldSelector();
     idRef.setFieldPath("metadata.uid");
     idFile.setFieldRef(idRef);
     idFile.setPath("pod.uid");

     downwardAPIVolumeSource.addItemsItem(podNameFile);
     downwardAPIVolumeSource.addItemsItem(labelsFile);
     downwardAPIVolumeSource.addItemsItem(idFile);

     volume.setDownwardAPI(downwardAPIVolumeSource);
     return volume;
   }
}
