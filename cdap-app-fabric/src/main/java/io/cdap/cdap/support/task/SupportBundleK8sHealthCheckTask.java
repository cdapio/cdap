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
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.metadata.RemoteHealthCheckFetcher;
import io.cdap.cdap.common.utils.DirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Collects support bundle kubernetes pods, nodes, events and logs from data fusion instance.
 */
public class SupportBundleK8sHealthCheckTask implements SupportBundleTask {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundlePipelineInfoTask.class);
  private static final Gson GSON = new GsonBuilder().create();
  private final CConfiguration cConfiguration;
  private final File basePath;
  private final RemoteHealthCheckFetcher remoteHealthCheckFetcher;

  @Inject
  public SupportBundleK8sHealthCheckTask(CConfiguration cConfiguration, File basePath,
                                         RemoteHealthCheckFetcher remoteHealthCheckFetcher) {
    this.cConfiguration = cConfiguration;
    this.basePath = basePath;
    this.remoteHealthCheckFetcher = remoteHealthCheckFetcher;
  }

  /**
   * Adds kubernetes pods, nodes, events and logs into files
   */
  @Override
  public void collect() throws IOException, NotFoundException {
    File supportBundleK8sHealthCheckFolder = new File(basePath, "health-check");
    DirUtils.mkdirs(supportBundleK8sHealthCheckFolder);

    String[] serviceNameList = cConfiguration.getStrings(Constants.HealthCheck.SERVICE_NAME_LIST);
    for (String serviceName : serviceNameList) {
      File supportBundleK8sHealthCheckServiceFolder = new File(supportBundleK8sHealthCheckFolder, serviceName);
      DirUtils.mkdirs(supportBundleK8sHealthCheckServiceFolder);
      String updatedServiceName = serviceName.toLowerCase().replace('-', '.');
      Map<String, Object> healthResponseData = remoteHealthCheckFetcher.getHealthDetails(updatedServiceName);


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
}
