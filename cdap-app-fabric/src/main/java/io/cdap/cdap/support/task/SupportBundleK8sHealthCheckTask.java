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

package io.cdap.cdap.support.task;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
      instanceLabel = cConfiguration.get(DEFAULT_INSTANCE_LABEL);
    }

    File supportBundleK8sHealthCheckFolder = new File(basePath, "health-check");
    DirUtils.mkdirs(supportBundleK8sHealthCheckFolder);

    for (NamespaceId namespaceId : namespaceIdList) {
      String namespace = namespaceId.getNamespace();
      String instanceName = getInstanceName(instanceLabel, namespace);
      File supportBundleK8sHealthCheckNameSpaceFolder = new File(supportBundleK8sHealthCheckFolder, namespace);
      DirUtils.mkdirs(supportBundleK8sHealthCheckNameSpaceFolder);

      V1ServiceList serviceInfoList = createHealthCheckServiceNameList(instanceName, namespace);
      Map<String, String> serviceNameToNodeLabelMap = mapServiceNametoNodeFieldLabel(serviceInfoList);
      for (V1Service serviceInfo : serviceInfoList.getItems()) {
        String serviceName = serviceInfo.getMetadata().getName();

        File supportBundleK8sHealthCheckServiceFolder =
          new File(supportBundleK8sHealthCheckNameSpaceFolder, serviceName);
        DirUtils.mkdirs(supportBundleK8sHealthCheckServiceFolder);
        String podLabelSelector = "";
        if (serviceInfo.getSpec().getSelector() != null) {
          podLabelSelector = serviceInfo.getSpec()
            .getSelector()
            .entrySet()
            .stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining(","));
        }
        Map<String, Object> healthResponseData =
          remoteHealthCheckFetcher.getHealthDetails(serviceName, namespace, instanceName, podLabelSelector,
                                                    serviceNameToNodeLabelMap.get(serviceName));


        writeKubeComponentInfo(healthResponseData, "pod", supportBundleK8sHealthCheckServiceFolder);
        writeKubeComponentInfo(healthResponseData, "node", supportBundleK8sHealthCheckServiceFolder);
        writeKubeComponentInfo(healthResponseData, "event", supportBundleK8sHealthCheckServiceFolder);

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

  private void writeKubeComponentInfo(Map<String, Object> appFabricData, String kubeComponentName,
                                      File supportBundleK8sHealthCheckServiceFolder) throws IOException {
    JsonArray kubeComponentList =
      GSON.fromJson((String) appFabricData.getOrDefault(kubeComponentName + "List", new JsonArray()), JsonArray.class);
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
    Map<String, String> podLabels = new HashMap<>();
    if (podList.getItems().size() > 0) {
      podLabels = podList.getItems().get(0).getMetadata().getLabels();
    }

    String instanceName = podLabels.getOrDefault(instanceLabel, null);
    if (instanceName == null) {
      throw new IllegalStateException("Missing instance label '" + instanceLabel + "' from pod labels.");
    }

    return instanceName;
  }

  private V1ServiceList createHealthCheckServiceNameList(String instanceName, String namespace) throws ApiException {
    List<String> healthCheckServiceNameList = new ArrayList<>();
    // Services are publish to K8s with a prefix
    String resourcePrefix = "cdap-" + instanceName + "-";
    String discoveryName = "health-check";
    V1ServiceList healthCheckServiceList = coreV1Api.listNamespacedService(namespace, null, null, null, null,
                                                                           "cdap.service="
                                                                             + resourcePrefix + discoveryName,
                                                                           1, null, null, null, null);

    return healthCheckServiceList;
  }

  private Map<String, String> mapServiceNametoNodeFieldLabel(V1ServiceList serviceInfoList) {
    Map<String, String> serviceNameToNodeLabelMap = new HashMap<>();
    Map<String, String> podNameToNodeMap = new HashMap<>();
    for (V1Pod pod : podList.getItems()) {
      if (pod.getMetadata() != null && pod.getMetadata().getOwnerReferences() != null && pod.getMetadata()
        .getOwnerReferences()
        .size() > 0) {
        podNameToNodeMap.put(pod.getMetadata().getOwnerReferences().get(0).getName(), pod.getSpec().getNodeName());
      }
    }
    for (V1Service service : serviceInfoList.getItems()) {
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
}
