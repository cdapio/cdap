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
import io.cdap.cdap.client.MetricsClient;
import io.cdap.cdap.client.NamespaceClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.authentication.client.AccessToken;
import io.cdap.cdap.security.authentication.client.AuthenticationClient;
import io.cdap.cdap.security.authentication.client.basic.BasicAuthenticationClient;
import io.cdap.cdap.support.job.SupportBundleJob;
import io.cdap.cdap.support.status.SupportBundleStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SupportBundleService {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleService.class);
  private static final Gson GSON = new GsonBuilder().create();
  private final SupportBundleStatus supportBundleStatus;
  private final List<String> serviceList;
  private final CConfiguration cConf;
  private final NamespaceClient namespaceClient;
  private final MetricsClient metricsClient;
  private final ProgramClient programClient;
  private final ApplicationClient applicationClient;

  @Inject
  public SupportBundleService(
      CConfiguration cConf, SupportBundleStatus supportBundleStatus) {
    this.cConf = cConf;
    this.serviceList =
      Arrays.asList(
        Constants.Service.APP_FABRIC_HTTP,
        Constants.Service.DATASET_EXECUTOR,
        Constants.Service.EXPLORE_HTTP_USER_SERVICE,
        Constants.Service.LOGSAVER,
        Constants.Service.MESSAGING_SERVICE,
        Constants.Service.METADATA_SERVICE,
        Constants.Service.METRICS,
        Constants.Service.METRICS_PROCESSOR,
        Constants.Service.RUNTIME,
        Constants.Service.TRANSACTION,
        "pipeline");
    this.supportBundleStatus = supportBundleStatus;
    String homePath = System.getProperty("user.home") + "/support/bundle";
    cConf.set(Constants.CFG_LOCAL_DATA_SUPPORT_BUNDLE_DIR, homePath);
    String instanceURI = System.getProperty("instanceUri", "localhost:11015");
    String hostName = instanceURI;
    int port = -1;
    if (instanceURI.contains(":")) {
      hostName = instanceURI.split(":")[0];
      port = Integer.parseInt(instanceURI.split(":")[1]);
    }
    // Obtain AccessToken
    AuthenticationClient authenticationClient = new BasicAuthenticationClient();
    authenticationClient.setConnectionInfo(hostName, port, false);
    AccessToken accessToken = null;
    try {
      accessToken = authenticationClient.getAccessToken();
    } catch (IOException e) {
      LOG.warn("Can not get access token ", e);
    }

    // Interact with the secure CDAP instance located at example.com, port 11015, with the provided
    // accessToken
    ClientConfig.Builder clientConfigBuilder =
      ClientConfig.builder().setConnectionConfig(new ConnectionConfig(hostName, port, false));
    if (accessToken != null) {
      clientConfigBuilder.setAccessToken(accessToken);
    }
    this.namespaceClient = new NamespaceClient(clientConfigBuilder.build());
    this.metricsClient = new MetricsClient(clientConfigBuilder.build());
    this.programClient = new ProgramClient(clientConfigBuilder.build());
    this.applicationClient = new ApplicationClient(clientConfigBuilder.build());
  }

  /** Generates support bundle */
  public void generateSupportBundle(
      String namespaceId,
      String appId,
      String runId,
      String workflowName,
      String uuid,
      Integer folderMaxNumber,
      Integer numOfRunNeeded) {
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

      supportBundleJob =
          new SupportBundleJob(
              GSON, supportBundleStatus, metricsClient, programClient, applicationClient);

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
            appId,
            serviceList,
            workflowName,
            basePath.getPath(),
            systemLogPath.getPath(),
            apps,
            numOfRunNeeded);
      }
      supportBundleStatus.setStatus("FINISHED");
      supportBundleStatus.setFinishTimestamp(System.currentTimeMillis());
      addToStatus(basePath.getPath());
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
