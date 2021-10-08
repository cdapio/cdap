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
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.NamespaceClient;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.support.job.SupportBundleJob;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class SupportBundleService {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleService.class);
  private static final Gson GSON = new GsonBuilder().create();
  private final SupportBundleStatus supportBundleStatus;
  private final CConfiguration cConf;
  private final NamespaceClient namespaceClient;
  private final ApplicationClient applicationClient;
  private final List<SupportBundleTaskFactory> supportBundleTaskFactoryList;

  @Inject
  public SupportBundleService(
      CConfiguration cConf,
      SupportBundleStatus supportBundleStatus,
      NamespaceClient namespaceClient,
      ApplicationClient applicationClient,
      List<SupportBundleTaskFactory> supportBundleTaskFactoryList) {
    this.cConf = cConf;
    this.supportBundleStatus = supportBundleStatus;
    this.namespaceClient = namespaceClient;
    this.applicationClient = applicationClient;
    this.supportBundleTaskFactoryList = supportBundleTaskFactoryList;
  }

  /** Generates support bundle */
  public void generateSupportBundle(
      HttpResponder responder,
      String namespaceId,
      String appId,
      String runId,
      String workflowName,
      Integer numOfRunNeeded) {
    String uuid = UUID.randomUUID().toString();
    int folderMaxNumber = cConf.getInt(Constants.SupportBundle.MAX_FOLDER_SIZE);
    File basePath = null;
    SupportBundleJob supportBundleJob = null;
    try {
      if (namespaceId != null) {
        NamespaceId namespace = new NamespaceId(namespaceId);
        if (!namespaceClient.exists(namespace)) {
          throw new NamespaceNotFoundException(namespace);
        }
      }
      JsonArray parameters = new JsonArray();
      Long startTs = System.currentTimeMillis();
      supportBundleStatus.setBundleId(uuid);
      supportBundleStatus.setStartTimestamp(startTs);
      if (namespaceId != null) {
        JsonObject parameter = new JsonObject();
        parameter.addProperty("namespace", namespaceId);
        parameters.add(parameter);
      }
      if (appId != null) {
        JsonObject parameter = new JsonObject();
        parameter.addProperty("application", appId);
        parameters.add(parameter);
      }
      if (runId != null) {
        JsonObject parameter = new JsonObject();
        parameter.addProperty("run_id", runId);
        parameters.add(parameter);
      }
      supportBundleStatus.setParameters(parameters);

      List<String> namespaceList = new ArrayList<>();
      // Puts all the files under the uuid path
      File baseDirectory = new File(cConf.get(Constants.CFG_LOCAL_DATA_SUPPORT_BUNDLE_DIR));
      int fileCount = 1;
      DirUtils.mkdirs(baseDirectory);
      if (baseDirectory.list() != null && baseDirectory.list().length > 0) {
        fileCount = baseDirectory.list().length;
      }

      // We want to keep consistent number of bundle to provide to customer
      if (fileCount >= folderMaxNumber) {
        File oldFilesDirectory = getOldestFolder(baseDirectory);
        deleteOldFolders(oldFilesDirectory);
      }
      basePath = new File(cConf.get(Constants.CFG_LOCAL_DATA_SUPPORT_BUNDLE_DIR), uuid);
      DirUtils.mkdirs(basePath);
      if (namespaceId == null) {
        namespaceList =
            namespaceClient.list().stream()
                .map(meta -> meta.getName())
                .collect(Collectors.toList());
      } else {
        namespaceList.add(namespaceId);
      }

      supportBundleJob = new SupportBundleJob(supportBundleStatus, supportBundleTaskFactoryList);

      for (String namespacesId : namespaceList) {
        NamespaceId namespace = new NamespaceId(namespacesId);
        List<ApplicationRecord> apps = new ArrayList<>();
        if (appId == null) {
          apps = applicationClient.list(namespace);
        } else {
          apps.add(
              new ApplicationRecord(applicationClient.get(new ApplicationId(namespaceId, appId))));
        }
        File systemLogPath = new File(basePath.getPath(), "system-log");
        DirUtils.mkdirs(systemLogPath);
        // Generates system log for user request
        supportBundleJob.executeTasks(
            namespaceId,
            workflowName,
            basePath.getPath(),
            systemLogPath.getPath(),
            apps,
            numOfRunNeeded);
      }
    } catch (Exception e) {
      LOG.error("Can not generate support bundle ", e);
      if (basePath != null) {
        supportBundleStatus.setStatus("FAILED");
        supportBundleStatus.setStatusDetails(e.getMessage());
        supportBundleStatus.setFinishTimestamp(System.currentTimeMillis());
        if (supportBundleJob != null) {
          addToStatus(basePath.getPath());
        }
      }
    }
    responder.sendString(
        HttpResponseStatus.OK, String.format("Support Bundle %s generated.", uuid));
  }

  /** Gets oldest folder from the root directory */
  private File getOldestFolder(File baseDirectory) {
    return DirUtils.listFiles(baseDirectory).stream()
        .reduce((f1, f2) -> f1.lastModified() < f2.lastModified() ? f1 : f2)
        .orElse(null);
  }

  /** Deletes old folders after certain number of folders exist */
  private void deleteOldFolders(File oldFilesDirectory) {
    try {
      DirUtils.deleteDirectoryContents(oldFilesDirectory);
    } catch (IOException e) {
      LOG.warn(String.format("Failed to clean up directory %s", oldFilesDirectory), e);
    }
  }

  /** Update status file */
  private synchronized void addToStatus(String basePath) {
    try (FileWriter statusFile = new FileWriter(new File(basePath, "status.json"))) {
      statusFile.write(GSON.toJson(supportBundleStatus));
      statusFile.flush();
    } catch (Exception e) {
      LOG.error("Can not update status file ", e);
    }
  }
}
