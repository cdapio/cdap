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

package io.cdap.cdap.support.services;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.RemoteNamespaceQueryClient;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.support.lib.SupportBundleFileNames;
import io.cdap.cdap.support.status.CollectionState;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.cdap.support.status.SupportBundleStatus;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Support bundle service to generate base path, uuid and trigger the job to execute tasks.
 */
public class SupportBundleService implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleService.class);
  private static final Gson GSON = new GsonBuilder().create();
  private final ExecutorService executorService;
  private final CConfiguration cConf;
  private final RemoteNamespaceQueryClient namespaceQueryClient;
  private final String localDir;

  @Inject
  SupportBundleService(CConfiguration cConf, RemoteNamespaceQueryClient namespaceQueryClient) {
    this.cConf = cConf;
    this.namespaceQueryClient = namespaceQueryClient;
    this.executorService = Executors.newFixedThreadPool(cConf.getInt(Constants.SupportBundle.MAX_THREADS),
                                                        Threads.createDaemonThreadFactory("perform-support-bundle"));
    this.localDir = this.cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR);
  }

  /**
   * Generates support bundle
   */
  public String generateSupportBundle(SupportBundleConfiguration supportBundleConfiguration) throws Exception {
    String namespace = supportBundleConfiguration.getNamespace();
    validNamespace(namespace);
    List<NamespaceId> namespaces = new ArrayList<>();
    if (namespace == null) {
      namespaces.addAll(
        namespaceQueryClient.list().stream().map(NamespaceMeta::getNamespaceId).collect(Collectors.toList()));
    } else {
      namespaces.add(new NamespaceId(namespace));
    }
    String uuid = UUID.randomUUID().toString();
    File basePath = new File(localDir, uuid);

    SupportBundleStatus supportBundleStatus =
      new SupportBundleStatus(uuid, System.currentTimeMillis(), supportBundleConfiguration,
                              CollectionState.IN_PROGRESS);
    try {
      // Puts all the files under the uuid path
      File baseDirectory = new File(localDir);
      DirUtils.mkdirs(baseDirectory);
      int fileCount = DirUtils.list(baseDirectory).size();

      // We want to keep consistent number of bundle to provide to customer
      int folderMaxNumber = cConf.getInt(Constants.SupportBundle.MAX_FOLDER_SIZE);
      if (fileCount >= folderMaxNumber) {
        File oldFilesDirectory = getOldestFolder(baseDirectory);
        deleteOldFolders(oldFilesDirectory);
      }
      DirUtils.mkdirs(basePath);

      SupportBundleStatus finishBundleStatus =
        new SupportBundleStatus(supportBundleStatus, "", CollectionState.FINISHED, System.currentTimeMillis());
      addToStatus(finishBundleStatus, basePath.getPath());
    } catch (Exception e) {
      SupportBundleStatus failedBundleStatus =
        new SupportBundleStatus(supportBundleStatus, e.getMessage(), CollectionState.FAILED,
                                System.currentTimeMillis());
      addToStatus(failedBundleStatus, basePath.getPath());
      throw new IllegalArgumentException("Failed to generate support bundle ", e);
    }
    return uuid;
  }

  /**
   * Gets oldest folder from the root directory
   */
  private File getOldestFolder(File baseDirectory) {
    List<File> uuidFiles = DirUtils.listFiles(baseDirectory).stream()
      .filter(file -> !file.getName().startsWith(".") && !file.isHidden() && file.isDirectory())
      .collect(Collectors.toList());
    return Collections.min(uuidFiles, Comparator.<File, Boolean>comparing(f1 -> {
      try {
        return getSingleBundleJson(f1).getStatus() != CollectionState.FAILED;
      } catch (Exception e) {
        throw new RuntimeException("Failed to get file status ", e);
      }
    }).thenComparing(File::lastModified));
  }

  /**
   * Deletes old folders after certain number of folders exist
   */
  private void deleteOldFolders(@Nullable File oldFilesDirectory) {
    try {
      DirUtils.deleteDirectoryContents(oldFilesDirectory);
    } catch (IOException e) {
      LOG.error("Failed to clean up directory {}", oldFilesDirectory, e);
    }
  }

  @Override
  public void close() {
    this.executorService.shutdown();
  }

  /**
   * Update status file
   */
  private void addToStatus(SupportBundleStatus supportBundleStatus, String basePath) throws IOException {
    try (FileWriter statusFile = new FileWriter(new File(basePath, SupportBundleFileNames.STATUS_FILE_NAME))) {
      GSON.toJson(supportBundleStatus, statusFile);
      statusFile.flush();
    } catch (IOException e) {
      throw new IOException("Failed to update status file ", e);
    }
  }

  /**
   * Gets single support bundle status with uuid
   */
  public SupportBundleStatus getSingleBundleJson(File uuidFile) throws IllegalArgumentException, IOException {
    File statusFile = new File(uuidFile, SupportBundleFileNames.STATUS_FILE_NAME);
    if (!statusFile.exists()) {
      throw new IllegalArgumentException("Failed to find this status file");
    }
    return readStatusJson(statusFile);
  }

  private SupportBundleStatus readStatusJson(File statusFile) throws IOException {
    SupportBundleStatus supportBundleStatus;
    try (Reader reader = Files.newBufferedReader(statusFile.toPath(), StandardCharsets.UTF_8)) {
      supportBundleStatus = GSON.fromJson(reader, SupportBundleStatus.class);
    } catch (IOException e) {
      throw new IOException("Failed to read this status file ", e);
    }
    return supportBundleStatus;
  }

  private void validNamespace(String namespace) throws Exception {
    if (namespace != null) {
      NamespaceId namespaceId = new NamespaceId(namespace);
      if (!namespaceQueryClient.exists(namespaceId)) {
        throw new NamespaceNotFoundException(namespaceId);
      }
    }
  }
}
