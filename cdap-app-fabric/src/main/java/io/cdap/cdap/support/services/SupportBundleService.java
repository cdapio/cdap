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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Support bundle service to generate base path, uuid and trigger the job to execute tasks
 */
public class SupportBundleService implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleService.class);
  private static final Gson GSON = new GsonBuilder().create();
  private final ExecutorService executorService;
  private final CConfiguration cConf;
  private final RemoteNamespaceQueryClient namespaceQueryClient;
  private final String localDir;

  @Inject
  public SupportBundleService(CConfiguration cConf,
                              RemoteNamespaceQueryClient namespaceQueryClient) {
    this.cConf = cConf;
    this.namespaceQueryClient = namespaceQueryClient;
    this.executorService = Executors.newFixedThreadPool(
    cConf.getInt(Constants.SupportBundle.MAX_THREADS),
    Threads.createDaemonThreadFactory("perform-support-bundle"));
    this.localDir = this.cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR);
  }

  /**
   * Generates support bundle
   */
  public String generateSupportBundle(SupportBundleConfiguration supportBundleConfiguration) throws Exception {
    SupportBundleStatus supportBundleStatus = new SupportBundleStatus();
    String uuid = UUID.randomUUID().toString();
    int folderMaxNumber = cConf.getInt(Constants.SupportBundle.MAX_FOLDER_SIZE);
    File basePath = new File(localDir, uuid);
    String namespaceId = supportBundleConfiguration.getNamespaceId();
    if (namespaceId != null) {
      NamespaceId namespace = new NamespaceId(namespaceId);
      if (!namespaceQueryClient.exists(namespace)) {
        throw new NamespaceNotFoundException(namespace);
      }
    }
    try {
      Long startTs = System.currentTimeMillis();
      supportBundleStatus.setBundleId(uuid);
      supportBundleStatus.setStartTimestamp(startTs);

      supportBundleStatus.setParameters(supportBundleConfiguration);

      List<String> namespaceList = new ArrayList<>();
      if (namespaceId == null) {
        namespaceList.addAll(
        namespaceQueryClient.list().stream()
        .map(NamespaceMeta::getName)
        .collect(Collectors.toList()));
      } else {
        namespaceList.add(namespaceId);
      }
      // Puts all the files under the uuid path
      File baseDirectory = new File(localDir);
      DirUtils.mkdirs(baseDirectory);
      int fileCount = DirUtils.list(baseDirectory).size();

      // We want to keep consistent number of bundle to provide to customer
      if (fileCount >= folderMaxNumber) {
        File oldFilesDirectory = getOldestFolder(baseDirectory);
        deleteOldFolders(oldFilesDirectory);
      }
      DirUtils.mkdirs(basePath);
      //TODO Execute tasks in parallel
    } catch (Exception e) {
      LOG.error("Can not generate support bundle ", e);
      supportBundleStatus.setStatus(CollectionState.FAILED);
      supportBundleStatus.setStatusDetails(e.getMessage());
      supportBundleStatus.setFinishTimestamp(System.currentTimeMillis());
      addToStatus(supportBundleStatus, basePath.getPath());
      throw new Exception("Can not generate support bundle ", e);
    }
    return uuid;
  }

  /**
   * Gets oldest folder from the root directory
   */
  private File getOldestFolder(File baseDirectory) throws RuntimeException {
    File[] uuidFiles = baseDirectory.listFiles((dir, name) -> !name.startsWith(".")
    && !dir.isHidden() && dir.isDirectory());
    return Collections.min(Arrays.asList(uuidFiles),
                           Comparator.<File, Boolean>comparing(
                           f1 -> {
                             try {
                               return getSingleBundleJson(f1).getStatus()
                               != CollectionState.FAILED;
                             } catch (Exception e) {
                               throw new RuntimeException("Can not get file status ", e);
                             }
                           })
                           .thenComparing(File::lastModified));
  }

  /**
   * Deletes old folders after certain number of folders exist
   */
  private void deleteOldFolders(
  @Nullable
  File oldFilesDirectory) {
    try {
      DirUtils.deleteDirectoryContents(oldFilesDirectory);
    } catch (IOException e) {
      LOG.error("Failed to clean up directory {}", oldFilesDirectory, e);
    }
  }

  public void close() {
    this.executorService.shutdown();
  }

  /**
   * Update status file
   */
  private void addToStatus(SupportBundleStatus supportBundleStatus,
                           String basePath) throws Exception {
    try (FileWriter statusFile = new FileWriter(
    new File(basePath, SupportBundleFileNames.statusFileName))) {
      GSON.toJson(supportBundleStatus, statusFile);
      statusFile.flush();
    } catch (Exception e) {
      LOG.error("Can not update status file ", e);
      throw new Exception("Can not update status file ", e);
    }
  }

  /**
   * Gets single support bundle status with uuid
   */
  public SupportBundleStatus getSingleBundleJson(File uuidFile) throws Exception {
    File statusFile = new File(uuidFile, SupportBundleFileNames.statusFileName);
    if (!statusFile.exists()) {
      LOG.error("Can not find this status file {} ", uuidFile.getName());
      throw new Exception("Can not find this status file");
    }
    return readStatusJson(statusFile);
  }

  private SupportBundleStatus readStatusJson(File statusFile) throws Exception {
    SupportBundleStatus supportBundleStatus = new SupportBundleStatus();
    try (Reader reader = Files.newBufferedReader(statusFile.toPath(), StandardCharsets.UTF_8)) {
      supportBundleStatus = GSON.fromJson(reader, SupportBundleStatus.class);
    } catch (Exception e) {
      LOG.error("Can not read status file ", e);
      throw new Exception("Can not read this status file ", e);
    }
    return supportBundleStatus;
  }
}
