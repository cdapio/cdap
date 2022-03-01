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

package io.cdap.cdap.support.services;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.RemoteNamespaceQueryClient;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.support.SupportBundleTaskConfiguration;
import io.cdap.cdap.support.job.SupportBundleJob;
import io.cdap.cdap.support.lib.SupportBundleFileNames;
import io.cdap.cdap.support.status.CollectionState;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Support bundle service to generate base path, uuid and trigger the job to execute tasks.
 */
public class SupportBundleGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleGenerator.class);
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, Boolean>>() {
  }.getType();
  private final Set<SupportBundleTaskFactory> taskFactories;
  private final CConfiguration cConf;
  private final RemoteNamespaceQueryClient namespaceQueryClient;
  private final String localDir;
  private final List<String> serviceList;
  private final List<String> listOfFileNames;

  @Inject
  SupportBundleGenerator(CConfiguration cConf, RemoteNamespaceQueryClient namespaceQueryClient,
                         @Named(Constants.SupportBundle.TASK_FACTORY) Set<SupportBundleTaskFactory> taskFactories) {
    this.cConf = cConf;
    this.namespaceQueryClient = namespaceQueryClient;
    this.localDir = cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR);
    this.taskFactories = taskFactories;
    this.serviceList = Arrays.asList(Constants.Service.APP_FABRIC_HTTP, Constants.Service.DATASET_EXECUTOR,
                                     Constants.Service.EXPLORE_HTTP_USER_SERVICE, Constants.Service.LOGSAVER,
                                     Constants.Service.MESSAGING_SERVICE, Constants.Service.METADATA_SERVICE,
                                     Constants.Service.METRICS, Constants.Service.METRICS_PROCESSOR,
                                     Constants.Service.RUNTIME, Constants.Service.TRANSACTION, "pipeline");
    this.listOfFileNames = new ArrayList<>(serviceList);
    listOfFileNames.addAll(Arrays.asList("applicationFile", "runtimelog", "runtimeinfo"));
  }

  /**
   * Generates support bundle
   */
  public String generate(SupportBundleConfiguration config, ExecutorService executorService) throws Exception {
    NamespaceId namespace = Optional.ofNullable(config.getNamespace()).map(NamespaceId::new).orElse(null);
    List<NamespaceId> namespaces = new ArrayList<>();

    if (namespace == null) {
      namespaces.addAll(
        namespaceQueryClient.list().stream().map(NamespaceMeta::getNamespaceId).collect(Collectors.toList()));
    } else {
      namespaces.add(validNamespace(namespace));
    }

    String uuid = UUID.randomUUID().toString();
    File uuidPath = new File(localDir, uuid);
    DirUtils.mkdirs(uuidPath);

    // Puts all the files under the uuid path
    File baseDirectory = new File(localDir);
    DirUtils.mkdirs(baseDirectory);
    deleteOldFoldersIfExceedLimit(baseDirectory);

    SupportBundleStatus supportBundleStatus = SupportBundleStatus.builder()
      .setBundleId(uuid)
      .setStartTimestamp(System.currentTimeMillis())
      .setStatus(CollectionState.IN_PROGRESS)
      .setParameters(config)
      .build();
    addToStatus(supportBundleStatus, uuidPath.getPath());

    SupportBundleJob supportBundleJob =
      new SupportBundleJob(taskFactories, executorService, cConf, supportBundleStatus);
    SupportBundleTaskConfiguration supportBundleTaskConfiguration =
      new SupportBundleTaskConfiguration(config, uuid, uuidPath, namespaces, supportBundleJob);

    try {
      executorService.execute(() -> supportBundleJob.generateBundle(supportBundleTaskConfiguration));
    } catch (Exception e) {
      LOG.error("Failed to finish execute tasks", e);
      SupportBundleStatus failedBundleStatus = SupportBundleStatus.builder(supportBundleStatus)
        .setStatus(CollectionState.FAILED)
        .setFinishTimestamp(System.currentTimeMillis())
        .setStatusDetails(e.getMessage())
        .build();
      addToStatus(failedBundleStatus, uuidPath.getPath());
    }

    return uuid;
  }

  /**
   * Gets single support bundle status with uuid
   */
  @VisibleForTesting
  SupportBundleStatus getBundleStatus(File uuidFile) throws IllegalArgumentException, IOException {
    File statusFile = new File(uuidFile, SupportBundleFileNames.STATUS_FILE_NAME);
    if (!statusFile.exists()) {
      throw new IllegalArgumentException("Failed to find this status file");
    }
    return readStatusJson(statusFile);
  }

  /**
   * Deletes old folders after certain number of folders exist
   */
  @VisibleForTesting
  void deleteOldFoldersIfExceedLimit(File baseDirectory) throws IOException {
    int fileCount = DirUtils.list(baseDirectory).size();
    // We want to keep consistent number of bundle to provide to customer
    int folderMaxNumber = cConf.getInt(Constants.SupportBundle.MAX_FOLDER_SIZE);
    if (fileCount >= folderMaxNumber) {
      File oldFilesDirectory = getOldestFolder(baseDirectory);
      DirUtils.deleteDirectoryContents(oldFilesDirectory);
    }
  }

  /**
   * Deletes old folders after certain number of folders exist
   */
  public void deleteOldFolders(File oldFilesDirectory) {
    String[] entries = oldFilesDirectory.list();
    if (entries != null && entries.length > 0) {
      for (String s : entries) {
        File currentFile = new File(oldFilesDirectory.getPath(), s);
        // Recursive the full directory and delete all old files
        if (currentFile.isDirectory()) {
          deleteOldFolders(currentFile);
        } else {
          currentFile.delete();
        }
      }
    }
    oldFilesDirectory.delete();
  }

  public JsonObject getSingleBundleJson(File uuidFile) throws IOException {
    JsonObject singleBundleJson = new JsonObject();
    singleBundleJson.addProperty("bundleId", uuidFile.getName());
    String basePath = cConf.get(Constants.CFG_LOCAL_DATA_SUPPORT_BUNDLE_DIR) + "/" + uuidFile.getName();
    File statusFile = new File(basePath + "/status.json");
    if (!statusFile.exists()) {
      JsonArray pipelinesArray = new JsonArray();
      File[] pipelineFiles =
        uuidFile.listFiles((dir, name) -> !name.startsWith(".") && !dir.isHidden() && dir.isDirectory());
      if (pipelineFiles != null && pipelineFiles.length > 0) {
        for (File pipelineFile : pipelineFiles) {
          if (!pipelineFile.getAbsoluteFile().getName().equals("system-log")) {
            File statusFileUnderPipeline = new File(pipelineFile.getPath() + "/status.json");
            if (statusFileUnderPipeline.exists()) {
              JsonObject singlePipelineJson = new JsonObject();
              singlePipelineJson.addProperty("application_name", pipelineFile.getAbsoluteFile().getName());
              readStatusJson(statusFileUnderPipeline, singlePipelineJson);
              pipelinesArray.add(singlePipelineJson);
            }
          }
        }
      }
      singleBundleJson.add("pipelines", pipelinesArray);
    } else {
      readStatusJson(statusFile, singleBundleJson);
    }
    return singleBundleJson;
  }

  public void readStatusJson(File statusFile, JsonObject jsonFile) throws IOException {
    try (Reader reader = Files.newBufferedReader(statusFile.toPath(), StandardCharsets.UTF_8)) {
      Map<String, Boolean> statusMap = GSON.fromJson(reader, MAP_TYPE);
      for (String fileName : listOfFileNames) {
        jsonFile.addProperty(fileName, statusMap.getOrDefault(fileName, false));
      }
    }
  }

  public JsonObject getFilesName(File file) {
    JsonObject singlePipelineJson = new JsonObject();
    singlePipelineJson.addProperty("application_name", file.getAbsoluteFile().getName());
    if (file.isDirectory()) {
      File[] dataFiles = file.listFiles((dir, name) -> !name.startsWith(".") && !dir.isHidden() && dir.isDirectory());
      if (dataFiles != null && dataFiles.length > 0) {
        JsonArray fileArray = new JsonArray();
        for (File dataFile : dataFiles) {
          JsonPrimitive fileJson = new JsonPrimitive(dataFile.getName());
          fileArray.add(fileJson);
        }
        singlePipelineJson.add("file_names", fileArray);
      }
    }
    return singlePipelineJson;
  }

  /**
   * Gets oldest folder from the root directory
   */
  private File getOldestFolder(File baseDirectory) {
    List<File> uuidFiles = DirUtils.listFiles(baseDirectory)
      .stream()
      .filter(file -> !file.getName().startsWith(".") && !file.isHidden() && file.isDirectory())
      .collect(Collectors.toList());
    return Collections.min(uuidFiles, Comparator.<File, Boolean>comparing(f1 -> {
      try {
        return getBundleStatus(f1).getStatus() != CollectionState.FAILED;
      } catch (Exception e) {
        throw new RuntimeException("Failed to get file status ", e);
      }
    }).thenComparing(File::lastModified));
  }

  /**
   * Update status file
   */
  private void addToStatus(SupportBundleStatus supportBundleStatus, String basePath) throws IOException {
    try (FileWriter statusFile = new FileWriter(new File(basePath, SupportBundleFileNames.STATUS_FILE_NAME))) {
      GSON.toJson(supportBundleStatus, statusFile);
    }
  }

  private SupportBundleStatus readStatusJson(File statusFile) throws IOException {
    SupportBundleStatus supportBundleStatus;
    try (Reader reader = Files.newBufferedReader(statusFile.toPath(), StandardCharsets.UTF_8)) {
      supportBundleStatus = GSON.fromJson(reader, SupportBundleStatus.class);
    }
    return supportBundleStatus;
  }

  private NamespaceId validNamespace(NamespaceId namespace) throws Exception {
    if (!namespaceQueryClient.exists(namespace)) {
      throw new NamespaceNotFoundException(namespace);
    }

    return namespace;
  }
}
