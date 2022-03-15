/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.task;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.metadata.RemoteHealthCheckFetcher;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.id.NamespaceId;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceList;
import io.kubernetes.client.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Collects support bundle kubernetes pods, nodes, events and logs from data fusion instance.
 */
public class SupportBundleK8sHealthCheckTask implements SupportBundleTask {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundlePipelineInfoTask.class);
  private static final Gson GSON = new GsonBuilder().create();
  private static final String INSTANCE_LABEL = "master.environment.k8s.instance.label";
  private static final String DEFAULT_INSTANCE_LABEL = "cdap.instance";

  private final CConfiguration cConfiguration;
  private final File basePath;
  private final List<NamespaceId> namespaceIdList;
  private final RemoteHealthCheckFetcher remoteHealthCheckFetcher;
  private ApiClient apiClient;
  private CoreV1Api coreV1Api;
  private V1PodList podList;

  @Inject
  public SupportBundleK8sHealthCheckTask(CConfiguration cConfiguration, File basePath,
                                         List<NamespaceId> namespaceIdList,
                                         RemoteHealthCheckFetcher remoteHealthCheckFetcher) {
    this.cConfiguration = cConfiguration;
    this.basePath = basePath;
    this.namespaceIdList = namespaceIdList;
    this.remoteHealthCheckFetcher = remoteHealthCheckFetcher;
    try {
      apiClient = Config.defaultClient();
      coreV1Api = new CoreV1Api(apiClient);
    } catch (Exception e) {
      // should not happen
      throw new RuntimeException("Exception while getting coreV1Api", e);
    }
  }

  /**
   * Adds kubernetes pods, nodes, events and logs into files
   */
  @Override
  public void collect() throws IOException, NotFoundException, ApiException {
    // Get the instance label to setup prefix for K8s services
    String instanceLabel = cConfiguration.get(INSTANCE_LABEL);
    if (instanceLabel == null) {
      instanceLabel = DEFAULT_INSTANCE_LABEL;
    }

    File supportBundleK8sHealthCheckFolder = new File(basePath, "health-check");
    DirUtils.mkdirs(supportBundleK8sHealthCheckFolder);

    for (NamespaceId namespaceId : namespaceIdList) {
      String namespace = namespaceId.getNamespace();
      String instanceName = getInstanceName(instanceLabel, namespace);
      File supportBundleK8sHealthCheckNameSpaceFolder = new File(supportBundleK8sHealthCheckFolder, namespace);
      DirUtils.mkdirs(supportBundleK8sHealthCheckNameSpaceFolder);

      List<V1Service> serviceInfoList = createHealthCheckServiceNameList(namespace);
      Map<String, String> serviceNameToNodeLabelMap = mapServiceNameToNodeFieldLabel(serviceInfoList);
      Map<String, V1PodList> serviceNameToPodInfoListMap = mapServiceNameToPodInfo(serviceInfoList);
      for (V1Service serviceInfo : serviceInfoList) {
        String serviceName = serviceInfo.getMetadata().getName();
        File supportBundleK8sHealthCheckServiceFolder =
          new File(supportBundleK8sHealthCheckNameSpaceFolder, serviceName);
        DirUtils.mkdirs(supportBundleK8sHealthCheckServiceFolder);

        V1PodList v1PodList = serviceNameToPodInfoListMap.getOrDefault(serviceName, new V1PodList());
        JsonArray podInfoArray = getPodInfoArray(v1PodList);
        JsonArray nodeInfoArray = getNodeInfoArray(serviceNameToNodeLabelMap.get(serviceName));

        Map<String, Object> healthResponseData = remoteHealthCheckFetcher.getHealthDetails(serviceName, instanceName);


        writeKubeComponentInfo(podInfoArray, "pod", supportBundleK8sHealthCheckServiceFolder);
        writeKubeComponentInfo(nodeInfoArray, "node", supportBundleK8sHealthCheckServiceFolder);

        String heapDump = (String) healthResponseData.getOrDefault("heapDump", "");
        File heapDumpPath = new File(supportBundleK8sHealthCheckServiceFolder, "heapDump.txt");
        try (FileWriter heapDumpFile = new FileWriter(heapDumpPath)) {
          GSON.toJson(heapDump, heapDumpFile);
        }

        String threadDump = (String) healthResponseData.getOrDefault("threadDump", "");
        File threadDumpPath = new File(supportBundleK8sHealthCheckServiceFolder, "threadDump.txt");
        try (FileWriter threadDumpFile = new FileWriter(threadDumpPath)) {
          GSON.toJson(threadDump, threadDumpFile);
        }
      }
    }
  }

  private void writeKubeComponentInfo(JsonArray kubeComponentList, String kubeComponentName,
                                      File supportBundleK8sHealthCheckServiceFolder) throws IOException {
    for (JsonElement kubeComponentInfo : kubeComponentList) {
      JsonObject kubeComponentObject = kubeComponentInfo.getAsJsonObject();
      String kubeComponentObjectName = kubeComponentObject.get("name").getAsString();

      File kubeComponentPath = new File(supportBundleK8sHealthCheckServiceFolder,
                                        kubeComponentObjectName + "-" + kubeComponentName + "-info.txt");
      try (FileWriter kubeComponentFile = new FileWriter(kubeComponentPath)) {
        GSON.toJson(kubeComponentObject, kubeComponentFile);
      }
    }
  }

  private String getInstanceName(String instanceLabel, String namespace) throws ApiException {
    this.podList = coreV1Api.listNamespacedPod(namespace, null, null, null, null, null, null, null, null, null, null);
    //Find which pod has instance label inside the pod metadata
    V1Pod pod = podList.getItems()
      .stream()
      .filter(
        v1Pod -> v1Pod.getMetadata().getLabels() != null && v1Pod.getMetadata().getLabels().containsKey(instanceLabel))
      .findFirst()
      .orElse(null);

    if (pod == null) {
      throw new IllegalStateException("Missing instance label '" + instanceLabel);
    }

    String instanceName = pod.getMetadata().getLabels().get(instanceLabel);
    return instanceName;
  }

  private List<V1Service> createHealthCheckServiceNameList(String namespace) throws ApiException {
    // Services are publish to K8s with a prefix
    String serviceLabel = "cdap.service";
    String healthCheckDiscoveryPrefixName = "health";
    V1ServiceList serviceList =
      coreV1Api.listNamespacedService(namespace, null, null, null, null, null, null, null, null, null, null);

    return serviceList.getItems()
      .stream()
      .filter(v1Service -> v1Service.getMetadata().getLabels() != null && v1Service.getMetadata()
        .getLabels()
        .containsKey(serviceLabel) && v1Service.getMetadata()
        .getLabels()
        .get(serviceLabel)
        .contains(healthCheckDiscoveryPrefixName))
      .collect(Collectors.toList());
  }

  private Map<String, String> mapServiceNameToNodeFieldLabel(List<V1Service> serviceInfoList) {
    Map<String, String> serviceNameToNodeLabelMap = new HashMap<>();
    Map<String, String> podNameToNodeMap = new HashMap<>();
    for (V1Pod pod : podList.getItems()) {
      if (pod.getMetadata() != null && pod.getMetadata().getOwnerReferences() != null && pod.getMetadata()
        .getOwnerReferences()
        .size() > 0) {
        podNameToNodeMap.put(pod.getMetadata().getOwnerReferences().get(0).getName(), pod.getSpec().getNodeName());
      }
    }
    for (V1Service service : serviceInfoList) {
      String serviceName = service.getMetadata().getName();
      if (service.getMetadata().getOwnerReferences() != null && service.getMetadata().getOwnerReferences().size() > 0) {
        String podName = service.getMetadata().getOwnerReferences().get(0).getName();
        if (podNameToNodeMap.containsKey(podName)) {
          String fieldSelector = "metadata.name=" + podNameToNodeMap.get(podName);
          serviceNameToNodeLabelMap.put(serviceName, fieldSelector);
        }
      }
    }
    return serviceNameToNodeLabelMap;
  }

  private Map<String, V1PodList> mapServiceNameToPodInfo(List<V1Service> serviceInfoList) {
    Map<String, V1PodList> serviceNameToPodInfoListMap = new HashMap<>();
    Map<String, V1Pod> podInfoMap = new HashMap<>();
    for (V1Pod pod : podList.getItems()) {
      if (pod.getMetadata() != null && pod.getMetadata().getOwnerReferences() != null && pod.getMetadata()
        .getOwnerReferences()
        .size() > 0) {
        podInfoMap.put(pod.getMetadata().getOwnerReferences().get(0).getName(), pod);
      }
    }
    for (V1Service service : serviceInfoList) {
      String serviceName = service.getMetadata().getName();
      if (service.getMetadata().getOwnerReferences() != null && service.getMetadata().getOwnerReferences().size() > 0) {
        String podName = service.getMetadata().getOwnerReferences().get(0).getName();
        if (podInfoMap.containsKey(podName)) {
          if (!serviceNameToPodInfoListMap.containsKey(serviceName)) {
            serviceNameToPodInfoListMap.put(serviceName, new V1PodList());
          }
          serviceNameToPodInfoListMap.get(serviceName).addItemsItem(podInfoMap.get(podName));
        }
      }
    }
    return serviceNameToPodInfoListMap;
  }

  private JsonArray getPodInfoArray(V1PodList podList) {
    JsonArray podArray = new JsonArray();
    try {
      Set<String> activePods = podList.getItems()
        .stream()
        .map(V1Pod::getMetadata)
        .filter(Objects::nonNull)
        .map(V1ObjectMeta::getName)
        .collect(Collectors.toSet());
      for (V1Pod pod : podList.getItems()) {
        if (pod.getMetadata() == null || !activePods.contains(pod.getMetadata().getName())) {
          continue;
        }
        String podName = pod.getMetadata().getName();
        JsonObject podObject = new JsonObject();
        podObject.addProperty("name", podName);
        if (pod.getStatus() != null) {
          podObject.addProperty("status", pod.getStatus().getMessage());
        }
        String podLog =
          coreV1Api.readNamespacedPodLog(pod.getMetadata().getName(), pod.getMetadata().getNamespace(), null, null,
                                         null, null, null, null, null, null, null);

        podObject.addProperty("podLog", podLog);
        podArray.add(podObject);
      }
    } catch (ApiException e) {
      LOG.error("Can not obtain api client ", e);
    }
    return podArray;
  }

  private JsonArray getNodeInfoArray(String nodeFieldSelector) {
    JsonArray nodeArray = new JsonArray();
    try {
      V1NodeList nodeList = coreV1Api.listNode(null, null, null, nodeFieldSelector, null, null, null, null, null, null);
      Set<String> activeNodes = nodeList.getItems()
        .stream()
        .map(V1Node::getMetadata)
        .filter(Objects::nonNull)
        .map(V1ObjectMeta::getName)
        .collect(Collectors.toSet());
      for (V1Node node : nodeList.getItems()) {
        if (node.getMetadata() == null || !activeNodes.contains(node.getMetadata().getName())) {
          continue;
        }
        String nodeName = node.getMetadata().getName();
        JsonObject nodeObject = new JsonObject();
        nodeObject.addProperty("name", nodeName);
        if (node.getStatus() != null && node.getStatus().getConditions() != null && node.getStatus()
          .getConditions()
          .size() > 0) {
          nodeObject.addProperty("status", node.getStatus().getConditions().get(0).getStatus());
        }
        nodeArray.add(nodeObject);
      }
    } catch (ApiException e) {
      LOG.error("Can not obtain api client ", e);
    }
    return nodeArray;
  }
}
