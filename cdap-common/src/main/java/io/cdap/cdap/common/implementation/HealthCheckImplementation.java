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

package io.cdap.cdap.common.implementation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.Config;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class HealthCheckImplementation {
  private static final Logger LOG = LoggerFactory.getLogger(HealthCheckImplementation.class);
  private static final int MegaBytes = (1024 * 1024);

  private static final Gson GSON = new GsonBuilder().create();
  private final CConfiguration cConf;
  private ApiClient apiClient;
  private CoreV1Api coreV1Api;


  @Inject
  public HealthCheckImplementation(CConfiguration cConf) {
    this.cConf = cConf;
    try {
      apiClient = Config.defaultClient();
      coreV1Api = new CoreV1Api(apiClient);
    } catch (Exception e) {
      // should not happen
      throw new RuntimeException("Exception while getting coreV1Api", e);
    }
  }

  public HealthCheckResponse collect(String namespace, String podLabelSelector, String nodeFieldSelector) {
    JsonArray podArray = getPodInfoArray(podLabelSelector, namespace);
    JsonArray nodeArray = getNodeInfoArray(nodeFieldSelector);

    String threadDump = "";
    String heapDump = "";
    try {
      heapDump = getHeapDump();
      threadDump = getThreadDump();
    } catch (IOException e) {
      LOG.error("Can not obtain client", e);
    }
    return HealthCheckResponse.named("Health check with data")
      .withData("podList", GSON.toJson(podArray))
      .withData("nodeList", GSON.toJson(nodeArray))
      .withData("heapDump", heapDump)
      .withData("threadDump", threadDump)
      .up()
      .build();
  }

  private static String getHeapDump() throws IOException {
    JsonObject heapInfo = new JsonObject();
    long freeMemory;
    long totalMemory;
    long maxMemory;


    freeMemory = Runtime.getRuntime().freeMemory() / MegaBytes;
    totalMemory = Runtime.getRuntime().totalMemory() / MegaBytes;
    maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;

    long usedMemory = maxMemory - freeMemory;
    heapInfo.addProperty("freeMemory: ", freeMemory + "MB");
    heapInfo.addProperty("totalMemory: ", totalMemory + "MB");
    heapInfo.addProperty("usedMemory: ", usedMemory + "MB");
    heapInfo.addProperty("maxMemory: ", maxMemory + "MB");

    return GSON.toJson(heapInfo);
  }

  private static String getThreadDump() throws IOException {
    StringBuilder threadDump = new StringBuilder(System.lineSeparator());
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
    for (ThreadInfo threadInfo : threadInfos) {
      threadDump.append(threadInfo);
    }
    return threadDump.toString();
  }

  public JsonArray getPodInfoArray(String podLabelSelector, String namespace) {
    JsonArray podArray = new JsonArray();
    try {
      V1PodList podList =
        coreV1Api.listNamespacedPod(namespace, null, null, null, null, podLabelSelector, null, null, null, null, null);
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
