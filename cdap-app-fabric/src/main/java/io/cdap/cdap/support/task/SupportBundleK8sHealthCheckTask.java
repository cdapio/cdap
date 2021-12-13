/*
 * Copyright © 2021 Cask Data, Inc.
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
import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.id.NamespaceId;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.CoreV1Event;
import io.kubernetes.client.openapi.models.CoreV1EventList;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeList;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Collects support bundle kubernetes pods, nodes, events and logs from data fusion instance.
 */
public class SupportBundleK8sHealthCheckTask implements SupportBundleTask {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundlePipelineInfoTask.class);
  private static final Gson GSON = new GsonBuilder().create();
  private final File basePath;
  private final List<NamespaceId> namespaces;

  @Inject
  public SupportBundleK8sHealthCheckTask(File basePath, List<NamespaceId> namespaces) {
    this.basePath = basePath;
    this.namespaces = namespaces;
  }

  /**
   * Adds kubernetes pods, nodes, events and logs into files
   */
  @Override
  public void collect() throws IOException, NotFoundException, ApiException {
    ApiClient client = Config.defaultClient();
    CoreV1Api coreApi = new CoreV1Api(client);
    // Set a reasonable timeout for the watch.
    client.setReadTimeout((int) TimeUnit.MINUTES.toMillis(5));
    for (NamespaceId namespaceId : namespaces) {
      File podStatusPath = new File(basePath, namespaceId.getNamespace() + "pod-status");
      DirUtils.mkdirs(podStatusPath);
      V1PodList podList =
        coreApi.listNamespacedPod(namespaceId.getNamespace(), null, null, null, null, null, null, null, null, null,
                                  null);
      for (V1Pod pod : podList.getItems()) {
        try (FileWriter podFile = new FileWriter(podStatusPath)) {
          GSON.toJson(pod, podFile);
        }

        if (pod.getMetadata() != null) {
          File podLogPath = new File(basePath, pod.getMetadata().getName() + "-pod-log");
          String podLog =
            coreApi.readNamespacedPodLog(pod.getMetadata().getName(), pod.getMetadata().getNamespace(), null, null, null,
                                         null, null, null, null, null, null);
          try (FileWriter podLogFile = new FileWriter(podLogPath)) {
            podLogFile.write(podLog);
          }
        }
      }

      File nodeStatusPath = new File(basePath, "node-status");
      V1NodeList nodeList = coreApi.listNode(null, null, null, null, null, null, null, null, null, null);
      for (V1Node node : nodeList.getItems()) {
        try (FileWriter file = new FileWriter(nodeStatusPath)) {
          GSON.toJson(node, file);
        }
      }

      CoreV1EventList coreV1EventList =
        coreApi.listNamespacedEvent(namespaceId.getNamespace(), null, null, null, null, null, null, null, null,
                                    null, null);
      File eventStatusPath = new File(basePath, "event-status");
      DirUtils.mkdirs(podStatusPath);
      for (CoreV1Event event : coreV1EventList.getItems()) {
        try (FileWriter file = new FileWriter(eventStatusPath)) {
          GSON.toJson(event, file);
        }
      }
    }
  }
}
