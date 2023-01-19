/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Service that manages source control operations for a namespace.
 */
public class SourceControlManagementService {
  private static final Logger LOG = LoggerFactory.getLogger(SourceControlManagementService.class);

  private static final Gson GSON = ApplicationSpecificationAdapter
    .addTypeAdapters(new GsonBuilder())
    .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
    .create();

  private final Store store;
  private final SecureStore secureStore;
  private final SourceControlManageProvider sourceControlManageProvider;
  private final ApplicationClient applicationClient;


  @Inject
  SourceControlManagementService(CConfiguration cConf,
                                 Store store, SecureStore secureStore,
                                 SourceControlManageProvider sourceControlManageProvider,
                                 ApplicationClient applicationClient,
                                 ) {
    this.store = store;
    this.secureStore = secureStore;
    this.sourceControlManageProvider = sourceControlManageProvider;
    this.applicationClient = applicationClient;
  }

  /**
   * Validates the source control config for the given namespace.
   *
   * @param namespaceId the namespace whose config is validated
   * @return Boolean indicating if the source control config is valid for the namespace
   */
  public boolean hasValidSourceControlConfiguration(NamespaceId namespaceId) throws Exception {
    SourceControlManager sourceControlManager = sourceControlManageProvider.GetSourceControlManagerByType("git");
    // populate properties
    sourceControlManager.initialize(new Properties(), secureStore);
    // handle possible network exception here
    return sourceControlManager.validateConfig();
  }

  /**
   * Fetches cdap application config file paths from local repository
   *
   * @param namespaceId the namespace whose linked repository is scanned
   * @return List of file paths of possible cdap application configs.
   * We do not have any good way to validate if the json file has valid cdap config
   * without deserializing and validating each json which will be compute-intensive
   * @throws Exception
   */

  // Create a new class for encapsulating application <-> path mapping
  public Map<String, String> fetchApplicationsFromSCM(NamespaceId namespaceId) throws Exception {
    SourceControlManager sourceControlManager = sourceControlManageProvider.GetSourceControlManagerByType("git");
    // populate properties
    sourceControlManager.initialize(new Properties(), secureStore);
    Repository repository = sourceControlManager.getRepository();

    // get default branch from config
    // maybe we should split cloning into a different method ?
    repository.switchBranch("develop", false);

    Path rootPath = repository.getRootPath();

    Map<String, String> applicationPathMap = new HashMap<>();
    // traverse the directory and get all json matching some rule

    File[] filteredJsonFiles = findValidApplicationConfig(rootPath);

    Arrays.stream(filteredJsonFiles).forEach(
      file -> {
        applicationPathMap.put(file.getPath(), file.getName());
      }
    );

    return applicationPathMap;
  }

  /**
   * Updates cdap application config file in source control
   *
   * @param namespaceId   the namespace whose linked repository is scanned
   * @param applicationId the application whose config should be pushed
   * @throws Exception
   */
  public void pushApplicationConfig(NamespaceId namespaceId, ApplicationId applicationId) throws Exception {
    SourceControlManager sourceControlManager = sourceControlManageProvider.GetSourceControlManagerByType("git");
    // populate properties
    sourceControlManager.initialize(new Properties(), secureStore);
    Repository repository = sourceControlManager.getRepository();

    // get application config
    ApplicationDetail applicationDetail = applicationClient.get(applicationId);

    // get default branch from config
    // maybe we should split cloning into a different method ?
    repository.switchBranch("develop", false);

    Path rootPath = repository.getRootPath();
    Path applicationConfigPath = getApplicationConfigFilePath(applicationDetail, rootPath);

    FileWriter writer = new FileWriter(applicationConfigPath.toFile());
    writer.write(GSON.toJson(applicationDetail));

    repository.push(new CommitMeta("", "", new DateTime(), ""));
  }

  /**
   * Updates application config in cdap from scm
   *
   * @param namespaceId   the namespace whose linked repository is scanned
   * @param applicationId the application whose config should be pulled
   * @throws Exception
   */
  public void pullApplicationConfig(NamespaceId namespaceId, ApplicationId applicationId) throws Exception {
    SourceControlManager sourceControlManager = sourceControlManageProvider.GetSourceControlManagerByType("git");
    // populate properties
    sourceControlManager.initialize(new Properties(), secureStore);
    Repository repository = sourceControlManager.getRepository();

    // get application config
    ApplicationDetail applicationDetail = applicationClient.get(applicationId);

    // get default branch from config
    // maybe we should split cloning into a different method ?
    repository.switchBranch("develop", false);

    Path rootPath = repository.getRootPath();
    Path applicationConfigPath = getApplicationConfigFilePath(applicationDetail, rootPath);

    JsonReader reader = new JsonReader(new FileReader(applicationConfigPath.toFile()));
    ApplicationDetail updatedDetails = GSON.fromJson(reader, ApplicationDetail.class);

    if(updatedDetails.getAppVersion().equals(applicationDetail.getAppVersion())){
      LOG.debug("Nothing changed. no need to pull");
      return;
    }

    applicationClient.deploy();
  }

  private Path getApplicationConfigFilePath(ApplicationDetail applicationDetail, Path rootPath) {
    // we should be getting the current mapped path in application details
    // otherwise fall back to standard path
    // how to figure out pipeline type ?
    return rootPath.resolve(String.format("cdap/applications/%s/%s",
                                                          applicationDetail.getArtifact().getName(),
                                                          applicationDetail.getName()));
  }


  // iterator pattern ?
  // unit testing ?
  private File[] findValidApplicationConfig(Path basePath) {
    File baseDirectory = basePath.toFile();
    FileFilter jsonFilter = new AndFileFilter(
      new RegexFileFilter(".*\\.json$"),
      FileFileFilter.FILE
    );

    return baseDirectory.listFiles(jsonFilter);
  }

}