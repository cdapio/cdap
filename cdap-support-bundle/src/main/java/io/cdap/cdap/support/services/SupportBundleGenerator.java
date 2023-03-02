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
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.namespace.RemoteNamespaceQueryClient;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.support.SupportBundleTaskConfiguration;
import io.cdap.cdap.support.job.SupportBundleJob;
import io.cdap.cdap.support.lib.SupportBundleFileNames;
import io.cdap.cdap.support.lib.SupportBundleOperationStatus;
import io.cdap.cdap.support.lib.SupportBundleRequestFileList;
import io.cdap.cdap.support.status.CollectionState;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleTaskStatus;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support bundle service to generate base path, uuid and trigger the job to execute tasks.
 */
public class SupportBundleGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleGenerator.class);
  private static final Gson GSON = new Gson();
  private final Set<SupportBundleTaskFactory> taskFactories;
  private final CConfiguration cConf;
  private final RemoteNamespaceQueryClient namespaceQueryClient;
  private final String localDir;

  @Inject
  SupportBundleGenerator(CConfiguration cConf, RemoteNamespaceQueryClient namespaceQueryClient,
      @Named(Constants.SupportBundle.TASK_FACTORY) Set<SupportBundleTaskFactory> taskFactories) {
    this.cConf = cConf;
    this.namespaceQueryClient = namespaceQueryClient;
    this.localDir = cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR);
    this.taskFactories = taskFactories;
  }

  /**
   * Generates support bundle
   */
  public String generate(SupportBundleConfiguration config, ExecutorService executorService)
      throws Exception {
    NamespaceId namespace = Optional.ofNullable(config.getNamespace()).map(NamespaceId::new)
        .orElse(null);
    List<NamespaceId> namespaces = new ArrayList<>();

    if (namespace == null) {
      namespaces.addAll(
          namespaceQueryClient.list().stream().map(NamespaceMeta::getNamespaceId)
              .collect(Collectors.toList()));
    } else {
      namespaces.add(validNamespace(namespace));
    }
    // Puts all the files under the uuid path
    File baseDirectory = new File(localDir);
    DirUtils.mkdirs(baseDirectory);
    String uuid = UUID.randomUUID().toString();
    File uuidPath = new File(localDir, uuid);

    deleteOldFoldersIfExceedLimit(baseDirectory);
    DirUtils.mkdirs(uuidPath);

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
      executorService.execute(
          () -> supportBundleJob.generateBundle(supportBundleTaskConfiguration));
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
   * Check whether the prev bundle is still processing or not
   */
  @Nullable
  public String getInProgressBundle() throws IOException {
    File latestDirectory = getLatestFolder();
    if (latestDirectory == null) {
      return null;
    }
    SupportBundleStatus supportBundleStatus = getBundleStatus(latestDirectory);
    if (supportBundleStatus != null
        && supportBundleStatus.getStatus() == CollectionState.IN_PROGRESS) {
      return supportBundleStatus.getBundleId();
    }
    return null;
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
  public void deleteOldFoldersIfExceedLimit(File baseDirectory) throws IOException {
    int fileCount = DirUtils.list(baseDirectory).size();
    // We want to keep consistent number of bundle to provide to customer
    int folderMaxNumber = cConf.getInt(Constants.SupportBundle.MAX_FOLDER_SIZE);
    if (fileCount >= folderMaxNumber) {
      File oldFilesDirectory = getOldestFolder(baseDirectory);
      DirUtils.deleteDirectoryContents(oldFilesDirectory);
    }
  }

  /**
   * Deletes select folder
   */
  public void deleteBundle(String uuid) throws IOException, NotFoundException {
    File uuidFile = getUUIDFile(uuid);
    if (!uuidFile.exists()) {
      throw new NotFoundException(String.format("No such uuid '%s' in Support Bundle.", uuid));
    }
    DirUtils.deleteDirectoryContents(uuidFile);
  }

  /**
   * Get single support bundle overall status
   */
  public SupportBundleOperationStatus getBundle(String uuid) throws IOException, NotFoundException {
    File uuidFile = getUUIDFile(uuid);
    if (!uuidFile.exists()) {
      throw new NotFoundException(String.format("No such uuid '%s' in Support Bundle.", uuid));
    }

    File statusFile = new File(uuidFile, "status.json");
    if (!statusFile.exists()) {
      throw new NotFoundException(
          String.format("The status for this bundle: %s is not found.", uuid));
    }
    SupportBundleStatus bundleStatus = getBundleStatus(uuidFile);
    Set<SupportBundleTaskStatus> supportBundleTaskStatusSet = bundleStatus.getTasks();
    return new SupportBundleOperationStatus(uuidFile.getName(), bundleStatus.getStatus(),
        supportBundleTaskStatusSet);
  }

  /**
   * Archive support bundle into zip file
   */
  public String createBundleZipByRequest(String uuid, Path tmpPath,
      SupportBundleRequestFileList bundleRequestFileList)
      throws IOException, NotFoundException, NoSuchAlgorithmException {
    File uuidFile = getUUIDFile(uuid);
    if (!uuidFile.exists()) {
      throw new NotFoundException(String.format("This bundle id %s is not existed", uuid));
    }

    MessageDigest digest = MessageDigest.getInstance("SHA-256");

    try (ZipOutputStream zipOut = new ZipOutputStream(
        new DigestOutputStream(Files.newOutputStream(tmpPath, StandardOpenOption.TRUNCATE_EXISTING),
            digest))) {
      if (bundleRequestFileList.getFiles().isEmpty()) {
        // If file path is empty string which means we want to zip the whole bundle id folder
        BundleJarUtil.addToArchive(uuidFile, true, zipOut);
      } else {
        for (String filePath : bundleRequestFileList.getFiles()) {
          File requestFile = new File(uuidFile, filePath);
          if (requestFile.exists()) {
            ZipEntry entry = new ZipEntry(uuidFile.getName() + "/" + filePath);
            zipOut.putNextEntry(entry);
            Files.copy(requestFile.toPath(), zipOut);
            zipOut.closeEntry();
          }
        }
      }
    }
    return String.format("%s=%s", digest.getAlgorithm().toLowerCase(),
        Base64.getEncoder().encodeToString(digest.digest()));
  }

  /**
   * Get all bundles status
   */
  public List<SupportBundleOperationStatus> getAllBundleStatus() {
    File baseDirectory = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    if (!baseDirectory.exists()) {
      LOG.debug("No content in Support Bundle.");
      return new ArrayList<>();
    }
    List<SupportBundleOperationStatus> operationStatusList = new ArrayList<>();

    DirUtils.listFiles(baseDirectory)
        .stream()
        .filter(file -> !file.isHidden() && file.isDirectory())
        .forEach(uuidFile -> {
          SupportBundleOperationStatus operationStatus = new SupportBundleOperationStatus(
              uuidFile.getName(),
              CollectionState.NOT_FOUND,
              new HashSet<>());
          try {
            operationStatus = getBundle(uuidFile.getName());
          } catch (Exception e) {
            LOG.debug("Can not find status json: {}", uuidFile.getName());
          }
          operationStatusList.add(operationStatus);
        });
    return operationStatusList;
  }

  /**
   * Get uuid file
   */
  private File getUUIDFile(String uuid) {
    File baseDirectory = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    return new File(baseDirectory, uuid);
  }

  /**
   * Gets oldest folder from the root directory
   */
  private File getOldestFolder(File baseDirectory) {
    List<File> uuidFiles = DirUtils.listFiles(baseDirectory)
        .stream()
        .filter(file -> !file.isHidden() && file.isDirectory())
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
   * Gets latest folder from the root directory
   */
  private File getLatestFolder() {
    File baseDirectory = new File(localDir);
    if (DirUtils.listFiles(baseDirectory, f -> !f.isHidden() && f.isDirectory()).isEmpty()) {
      return null;
    }
    List<File> uuidFiles = DirUtils.listFiles(baseDirectory).stream()
        .filter(file -> !file.getName().startsWith(".") && !file.isHidden() && file.isDirectory())
        .collect(Collectors.toList());
    return Collections.max(uuidFiles, Comparator.comparing(File::lastModified));
  }

  /**
   * Update status file
   */
  private void addToStatus(SupportBundleStatus bundleStatus, String basePath) throws IOException {
    try (FileWriter statusFile = new FileWriter(
        new File(basePath, SupportBundleFileNames.STATUS_FILE_NAME))) {
      GSON.toJson(bundleStatus, statusFile);
    }
  }

  /**
   * read status file
   */
  private SupportBundleStatus readStatusJson(File statusFile) throws IOException {
    SupportBundleStatus bundleStatus;
    try (Reader reader = Files.newBufferedReader(statusFile.toPath(), StandardCharsets.UTF_8)) {
      bundleStatus = GSON.fromJson(reader, SupportBundleStatus.class);
    }
    return bundleStatus;
  }

  /**
   * valid if the namespace exists or not
   */
  private NamespaceId validNamespace(NamespaceId namespace) throws Exception {
    if (!namespaceQueryClient.exists(namespace)) {
      throw new NamespaceNotFoundException(namespace);
    }

    return namespace;
  }
}
