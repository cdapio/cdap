/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.sourcecontrol.operationrunner;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.FileUtils;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.ConfigFileWriteException;
import io.cdap.cdap.sourcecontrol.GitOperationException;
import io.cdap.cdap.sourcecontrol.InvalidApplicationConfigException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.RepositoryManagerFactory;
import io.cdap.cdap.sourcecontrol.SecureSystemReader;
import io.cdap.cdap.sourcecontrol.SourceControlAppConfigNotFoundException;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-Memory implementation for {@link SourceControlOperationRunner}.
 * Runs all git operation inside calling service.
 */
@Singleton
public class InMemorySourceControlOperationRunner extends
    AbstractIdleService implements SourceControlOperationRunner {
  // Gson for decoding request
  private static final Gson DECODE_GSON =
    new GsonBuilder().registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory()).create();
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final Logger LOG = LoggerFactory.getLogger(InMemorySourceControlOperationRunner.class);
  private final RepositoryManagerFactory repoManagerFactory;

  @Inject
  InMemorySourceControlOperationRunner(RepositoryManagerFactory repoManagerFactory) {
    this.repoManagerFactory = repoManagerFactory;
  }

  @Override
  public PushAppResponse push(PushAppOperationRequest pushRequest) throws NoChangesToPushException,
      AuthenticationConfigException {
    try (
        RepositoryManager repositoryManager = repoManagerFactory.create(
            pushRequest.getNamespaceId(),
            pushRequest.getRepositoryConfig())
    ) {
      try {
        repositoryManager.cloneRemote();
      } catch (GitAPIException | IOException e) {
        throw new GitOperationException(String.format("Failed to clone remote repository: %s",
                                                       e.getMessage()), e);
      }

      LOG.info("Pushing application configs for : {}", pushRequest.getApp().getName());

      //TODO: CDAP-20371, Add retry logic here in case the head at remote moved while we are doing push
      return SourceControlOperationUtils.writeAppDetailsAndPush(
          repositoryManager,
          Collections.singletonList(pushRequest.getApp()),
          pushRequest.getCommitDetails()
      ).get(0);
    }
  }

  @Override
  public void push(
      MultiPushAppOperationRequest pushRequest,
      Consumer<Collection<PushAppResponse>> consumer)
      throws SourceControlAppConfigNotFoundException, AuthenticationConfigException, NoChangesToPushException {
    try (RepositoryManager repositoryManager = repoManagerFactory.create(pushRequest.getNamespaceId(),
        pushRequest.getRepositoryConfig())) {
      try {
        repositoryManager.cloneRemote();
      } catch (GitAPIException | IOException e) {
        throw new GitOperationException(String.format("Failed to clone remote repository: %s",
            e.getMessage()), e);
      }

      List<PushAppResponse> responses = SourceControlOperationUtils.writeAppDetailsAndPush(
          repositoryManager,
          pushRequest.getApps(),
          pushRequest.getCommitDetails()
      );
      consumer.accept(responses);
    }
  }

  @Override
  public PullAppResponse<?> pull(PullAppOperationRequest pullRequest)
      throws NotFoundException, AuthenticationConfigException {
    AtomicReference<PullAppResponse<?>> response = new AtomicReference<>();
    pull(
        new MultiPullAppOperationRequest(
            pullRequest.getRepositoryConfig(),
            pullRequest.getApp().getNamespaceId(),
            ImmutableSet.of(pullRequest.getApp().getApplication())
        ),
        response::set
    );
    // it should never be null as if the application is not found we will get an exception
    return response.get();
  }

  @Override
  public void pull(MultiPullAppOperationRequest pullRequest, Consumer<PullAppResponse<?>> consumer)
      throws SourceControlAppConfigNotFoundException, AuthenticationConfigException {
    LOG.info("Cloning remote to pull applications {}", pullRequest.getApps());

    try (RepositoryManager repositoryManager = repoManagerFactory.create(pullRequest.getNamespace(),
        pullRequest.getRepositoryConfig())) {
      String commitId = repositoryManager.cloneRemote();

      for (String applicationName : pullRequest.getApps()) {
        PullAppResponse<?> response = pullSingle(repositoryManager, commitId, applicationName);
        consumer.accept(response);
      }
    } catch (GitAPIException | IOException e) {
      throw new GitOperationException(
          String.format("Failed to clone repository %s", e.getMessage()), e);
    }
  }

  private PullAppResponse<?> pullSingle(RepositoryManager repositoryManager, String commitId,
      String applicationName) throws SourceControlException, SourceControlAppConfigNotFoundException {
    String configFileName = SourceControlOperationUtils.generateConfigFileName(applicationName);
    Path appRelativePath = repositoryManager.getFileRelativePath(configFileName);
    Path filePathToRead = SourceControlOperationUtils.validateAppConfigRelativePath(repositoryManager, appRelativePath);
    if (!Files.exists(filePathToRead)) {
      throw new SourceControlAppConfigNotFoundException(applicationName, appRelativePath);
    }
    LOG.info("Getting file hash for application {}", applicationName);
    try {
      String fileHash = repositoryManager.getFileHash(appRelativePath, commitId);
      String contents = new String(Files.readAllBytes(filePathToRead), StandardCharsets.UTF_8);
      AppRequest<?> appRequest = DECODE_GSON.fromJson(contents, AppRequest.class);

      return new PullAppResponse<>(applicationName, fileHash, appRequest);
    } catch (GitAPIException e) {
      throw new GitOperationException(String.format("Failed to pull application %s: %s",
          applicationName, e.getMessage()), e);
    } catch (JsonSyntaxException e) {
      throw new InvalidApplicationConfigException(appRelativePath, e);
    } catch (NotFoundException e) {
      throw new SourceControlAppConfigNotFoundException(applicationName, appRelativePath);
    } catch (Exception e) {
      throw new SourceControlException(
          String.format("Failed to pull application %s.", applicationName), e);
    }
  }

  @Override
  public RepositoryAppsResponse list(NamespaceRepository nameSpaceRepository) throws
    AuthenticationConfigException, NotFoundException {
    try (RepositoryManager repositoryManager = repoManagerFactory.create(nameSpaceRepository.getNamespaceId(),
                                                                         nameSpaceRepository.getRepositoryConfig())) {
      String currentCommit = repositoryManager.cloneRemote();
      Path basePath = repositoryManager.getBasePath();

      if (!Files.exists(basePath)) {
        throw new NotFoundException(String.format("Missing repository base path %s", basePath));
      }

      FileFilter configFileFilter = getConfigFileFilter();
      List<File> configFiles = DirUtils.listFiles(basePath.toFile(), configFileFilter);
      List<RepositoryApp> responses = new ArrayList<>();

      // TODO CDAP-20420 Implement bulk fetch file hash in RepositoryManager
      for (File configFile : configFiles) {
        String applicationName = FileUtils.getNameWithoutExtension(configFile.getName());
        Path appConfigRelativePath = repositoryManager.getFileRelativePath(configFile.getName());
        try {
          String fileHash = repositoryManager.getFileHash(appConfigRelativePath, currentCommit);
          responses.add(new RepositoryApp(applicationName, fileHash));
        } catch (NotFoundException e) {
          // ignore file if not found in git tree
          LOG.warn("Skipping config file {}: {}", appConfigRelativePath, e.getMessage());
        }
      }
      return new RepositoryAppsResponse(responses);
    } catch (IOException | GitAPIException e) {
      throw new GitOperationException(String.format("Failed to list application configs in directory %s: %s",
                                                     nameSpaceRepository.getRepositoryConfig().getPathPrefix(),
                                                     e.getMessage()), e);
    }
  }


  /**
   * A helper function to get a {@link java.io.FileFilter} for application config files with following rules
   *    1. Filter non-symbolic link files
   *    2. Filter files with extension json
   */
  private FileFilter getConfigFileFilter() {
    return file -> {
      return Files.isRegularFile(file.toPath(), LinkOption.NOFOLLOW_LINKS) // only allow regular files
          && FileUtils.getExtension(file.getName()).equalsIgnoreCase("json"); // filter json extension
    };
  }

  @Override
  protected void startUp() throws Exception {
    SecureSystemReader.setAsSystemReader();
  }

  @Override
  protected void shutDown() throws Exception {
    // No-op.
  }
}

