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
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.authentication.client.AccessToken;
import io.cdap.cdap.security.authentication.client.AuthenticationClient;
import io.cdap.cdap.security.authentication.client.basic.BasicAuthenticationClient;
import io.cdap.cdap.support.conf.SupportBundleConfiguration;
import io.cdap.cdap.support.job.SupportBundleJob;
import io.cdap.cdap.support.status.SupportBundleCreationQueryParameters;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.TaskStatus;
import io.cdap.cdap.support.task.factory.SupportBundlePipelineInfoTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundleSystemLogTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SupportBundleService {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleService.class);
  private static final Gson GSON = new GsonBuilder().create();
  private static final List<SupportBundleTaskFactory> supportBundleTaskFactoryList = new ArrayList<>();
  private final ExecutorService executorService;
  private final SupportBundleStatus supportBundleStatus;
  private final CConfiguration cConf;
  private final NamespaceClient namespaceClient;
  private final ApplicationClient applicationClient;
  private final MetricsClient metricsClient;
  private final ProgramClient programClient;

  @Inject
  public SupportBundleService(
      CConfiguration cConf,
      SupportBundleStatus supportBundleStatus,
      SupportBundleSystemLogTaskFactory supportBundleSystemLogTaskFactory,
      SupportBundlePipelineInfoTaskFactory supportBundlePipelineInfoTaskFactory) {
    this.cConf = cConf;
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
    this.supportBundleTaskFactoryList.add(supportBundleSystemLogTaskFactory);
    this.supportBundleTaskFactoryList.add(supportBundlePipelineInfoTaskFactory);
    this.executorService =
        new ThreadPoolExecutor(0, 1, 5L, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
  }

  /** Generates support bundle */
  public String generateSupportBundle(SupportBundleConfiguration supportBundleConfiguration) {
    String uuid = UUID.randomUUID().toString();
    int folderMaxNumber = cConf.getInt(Constants.SupportBundle.MAX_FOLDER_SIZE);
    File basePath = new File(cConf.get(Constants.CFG_LOCAL_DATA_SUPPORT_BUNDLE_DIR), uuid);
    SupportBundleJob supportBundleJob =
        new SupportBundleJob(supportBundleStatus, supportBundleTaskFactoryList, applicationClient);
    String namespaceId = supportBundleConfiguration.getNamespaceId();
    String appId = supportBundleConfiguration.getAppId();
    String runId = supportBundleConfiguration.getRunId();
    String workflowName = supportBundleConfiguration.getWorkflowName();
    Integer numOfRunNeeded = supportBundleConfiguration.getNumOfRunLogNeeded();
    try {
      if (namespaceId != null) {
        NamespaceId namespace = new NamespaceId(namespaceId);
        if (!namespaceClient.exists(namespace)) {
          throw new NamespaceNotFoundException(namespace);
        }
      }
      Long startTs = System.currentTimeMillis();
      supportBundleStatus.setBundleId(uuid);
      supportBundleStatus.setStartTimestamp(startTs);
      SupportBundleCreationQueryParameters supportBundleCreationQueryParameters =
          new SupportBundleCreationQueryParameters();
      if (namespaceId != null) {
        supportBundleCreationQueryParameters.setNamespaceId(namespaceId);
      }
      if (appId != null) {
        supportBundleCreationQueryParameters.setAppId(appId);
      }
      if (runId != null) {
        supportBundleCreationQueryParameters.setRunId(runId);
      }
      if (namespaceId != null) {
        supportBundleCreationQueryParameters.setNamespaceId(namespaceId);
      }
      if (workflowName != null) {
        supportBundleCreationQueryParameters.setWorkflowName(workflowName);
      }
      if (numOfRunNeeded != null) {
        supportBundleCreationQueryParameters.setNumOfRunLog(numOfRunNeeded);
      }
      supportBundleStatus.setParameters(supportBundleCreationQueryParameters);

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
      DirUtils.mkdirs(basePath);
      if (namespaceId == null) {
        namespaceList.addAll(
            namespaceClient.list().stream()
                .map(meta -> meta.getName())
                .collect(Collectors.toList()));
      } else {
        namespaceList.add(namespaceId);
      }
      supportBundleConfiguration.setBasePath(basePath.getPath());
      supportBundleConfiguration.setNamespaceList(namespaceList);
      supportBundleConfiguration.setSupportBundleStatus(supportBundleStatus);
      supportBundleConfiguration.setProgramClient(programClient);
      supportBundleConfiguration.setApplicationClient(applicationClient);
      supportBundleConfiguration.setNamespaceClient(namespaceClient);
      supportBundleConfiguration.setMetricsClient(metricsClient);
      executorService.execute(() -> supportBundleJob.executeTasks(supportBundleConfiguration));
    } catch (Exception e) {
      LOG.error("Can not generate support bundle ", e);
      if (basePath != null) {
        supportBundleStatus.setStatus(TaskStatus.FAILED);
        supportBundleStatus.setStatusDetails(e.getMessage());
        supportBundleStatus.setFinishTimestamp(System.currentTimeMillis());
        if (supportBundleJob != null) {
          addToStatus(basePath.getPath());
        }
      }
    }
    return uuid;
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
