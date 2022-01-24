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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.metadata.RemoteAppFabricHealthCheckFetcher;
import io.kubernetes.client.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Collects support bundle kubernetes pods, nodes, events and logs from data fusion instance.
 */
public class SupportBundleK8sHealthCheckTask implements SupportBundleTask {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundlePipelineInfoTask.class);
  private static final Gson GSON = new GsonBuilder().create();
  private final File basePath;
  private final RemoteAppFabricHealthCheckFetcher remoteAppFabricHealthCheckFetcher;

  @Inject
  public SupportBundleK8sHealthCheckTask(File basePath,
                                         RemoteAppFabricHealthCheckFetcher remoteAppFabricHealthCheckFetcher) {
    this.basePath = basePath;
    this.remoteAppFabricHealthCheckFetcher = remoteAppFabricHealthCheckFetcher;
  }

  /**
   * Adds kubernetes pods, nodes, events and logs into files
   */
  @Override
  public void collect() throws IOException, NotFoundException, ApiException {
    Optional<Map<String, Object>> appFabricData = remoteAppFabricHealthCheckFetcher.getHealthDetails();
    if (appFabricData.isPresent()) {
      File supportBundleK8sHealthCheckFolder = new File(basePath, "health-check");
      DirUtils.mkdirs(supportBundleK8sHealthCheckFolder);

      writeKubeComponentInfo(appFabricData, "pod", supportBundleK8sHealthCheckFolder);
      writeKubeComponentInfo(appFabricData, "node", supportBundleK8sHealthCheckFolder);
      writeKubeComponentInfo(appFabricData, "event", supportBundleK8sHealthCheckFolder);

      String heapDump = (String) appFabricData.get().getOrDefault("heapDump", "");
      File heapDumpPath = new File(supportBundleK8sHealthCheckFolder, "heapDump.txt");
      try (FileWriter heapDumpFile = new FileWriter(heapDumpPath)) {
        GSON.toJson(heapDump, heapDumpFile);
      }

      String threadDump = (String) appFabricData.get().getOrDefault("threadDump", "");
      File threadDumpPath = new File(supportBundleK8sHealthCheckFolder, "threadDump.txt");
      try (FileWriter threadDumpFile = new FileWriter(threadDumpPath)) {
        GSON.toJson(threadDump, threadDumpFile);
      }
    }

  }

  private void writeKubeComponentInfo(Optional<Map<String, Object>> appFabricData, String kubeComponentName,
                                      File supportBundleK8sHealthCheckFolder) throws IOException {
    JsonArray kubeComponentList =
      GSON.fromJson((String) appFabricData.get().getOrDefault(kubeComponentName + "List", new JsonArray()),
                    JsonArray.class);
    for (JsonElement kubeComponentInfo : kubeComponentList) {
      JsonObject kubeComponentObject = kubeComponentInfo.getAsJsonObject();
      String kubeComponentObjectName = kubeComponentObject.get("name").getAsString();

      File kubeComponentPath =
        new File(supportBundleK8sHealthCheckFolder, kubeComponentObjectName + "-" + kubeComponentName + "-info.txt");
      try (FileWriter kubeComponentFile = new FileWriter(kubeComponentPath)) {
        GSON.toJson(kubeComponentObject, kubeComponentFile);
      }
    }
  }
}
