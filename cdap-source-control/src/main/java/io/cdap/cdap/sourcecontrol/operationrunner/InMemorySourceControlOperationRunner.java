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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.FileUtils;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ProtoConstraint;
import io.cdap.cdap.proto.ProtoConstraint.ConcurrencyConstraint;
import io.cdap.cdap.proto.ProtoConstraint.DelayConstraint;
import io.cdap.cdap.proto.ProtoConstraint.LastRunConstraint;
import io.cdap.cdap.proto.ProtoConstraint.TimeRangeConstraint;
import io.cdap.cdap.proto.ProtoConstraintCodec;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.ProtoTrigger.AndTrigger;
import io.cdap.cdap.proto.ProtoTrigger.OrTrigger;
import io.cdap.cdap.proto.ProtoTrigger.PartitionTrigger;
import io.cdap.cdap.proto.ProtoTrigger.ProgramStatusTrigger;
import io.cdap.cdap.proto.ProtoTrigger.TimeTrigger;
import io.cdap.cdap.proto.ProtoTriggerCodec;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.sourcecontrol.NamespaceConfigData;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.ConfigFileWriteException;
import io.cdap.cdap.sourcecontrol.GitOperationException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.RepositoryManagerFactory;
import io.cdap.cdap.sourcecontrol.SecureSystemReader;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    new GsonBuilder()
        .registerTypeAdapter(Trigger.class, new ProtoTriggerCodec())
        .registerTypeAdapter(ProtoTrigger.class, new ProtoTriggerCodec())
      .registerTypeAdapter(Constraint.class, new ConstraintCodec())
        .registerTypeAdapter(Requirements.class, new RequirementsCodec())
      .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory()).create();
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(Trigger.class, new ProtoTriggerCodec())
      .registerTypeAdapter(ProtoTrigger.class, new ProtoTriggerCodec())
      .registerTypeAdapter(Constraint.class, new ConstraintCodec())
      .registerTypeAdapter(Requirements.class, new RequirementsCodec()).setPrettyPrinting().create();
  private static final Logger LOG = LoggerFactory.getLogger(InMemorySourceControlOperationRunner.class);
  private final RepositoryManagerFactory repoManagerFactory;

  public static class TriggerCodec extends ProtoTriggerCodec {
    private static final Map<ProtoTrigger.Type, Class<? extends Trigger>> TYPE_TO_INTERNAL_TRIGGER =
        ImmutableMap.<ProtoTrigger.Type, Class<? extends Trigger>>builder()
            .put(ProtoTrigger.Type.TIME, TimeTrigger.class)
            .put(ProtoTrigger.Type.PARTITION, PartitionTrigger.class)
            .put(ProtoTrigger.Type.PROGRAM_STATUS, ProgramStatusTrigger.class)
            .put(ProtoTrigger.Type.AND, AndTrigger.class)
            .put(ProtoTrigger.Type.OR, OrTrigger.class)
            .build();

    public TriggerCodec() {
      super(TYPE_TO_INTERNAL_TRIGGER);
    }
  }

  public static class ConstraintCodec extends ProtoConstraintCodec {

    private static final Map<ProtoConstraint.Type, Class<? extends ProtoConstraint>> TYPE_TO_CONSTRAINT =
        ImmutableMap.<ProtoConstraint.Type, Class<? extends ProtoConstraint>>builder()
            .put(ProtoConstraint.Type.CONCURRENCY, ConcurrencyConstraint.class)
            .put(ProtoConstraint.Type.DELAY, DelayConstraint.class)
            .put(ProtoConstraint.Type.LAST_RUN, LastRunConstraint.class)
            .put(ProtoConstraint.Type.TIME_RANGE, TimeRangeConstraint.class)
            .build();

    public ConstraintCodec() {
      super(TYPE_TO_CONSTRAINT);
    }
  }

  public static class RequirementsCodec implements JsonDeserializer<Requirements>,
      JsonSerializer<Requirements> {

    public static final String DATASET_TYPES = "datasetTypes";
    public static final String CAPABILITIES = "capabilities";

    @Override
    public Requirements deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      return new Requirements(getSet(jsonObject, context, DATASET_TYPES),
          getSet(jsonObject, context, CAPABILITIES));
    }

    private Set<String> getSet(JsonObject jsonObject, JsonDeserializationContext context,
        String fieldName) {
      JsonElement jsonElement = jsonObject.get(fieldName);
      if (jsonElement == null) {
        return Collections.emptySet();
      }
      return context.deserialize(jsonElement, Set.class);
    }

    @Override
    public JsonElement serialize(Requirements src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject jsonObject = new JsonObject();
      jsonObject.add(DATASET_TYPES, context.serialize(src.getDatasetTypes()));
      jsonObject.add(CAPABILITIES, context.serialize(src.getCapabilities()));
      return jsonObject;
    }
  }

  @Inject
  InMemorySourceControlOperationRunner(RepositoryManagerFactory repoManagerFactory) {
    this.repoManagerFactory = repoManagerFactory;
  }


  @Override
  public PushNamespaceConfigResponse pushNamespaceConfig(
      PushNamespaceConfigOperationRequest pushNsRequest)
    throws NoChangesToPushException, AuthenticationConfigException {
    try (
        RepositoryManager repositoryManager = repoManagerFactory.create(
            pushNsRequest.getNamespaceId(), pushNsRequest.getRepositoryConfig())
    ) {
      try {
        repositoryManager.cloneRemote();
      } catch (GitAPIException | IOException e) {
        throw new GitOperationException(String.format("Failed to clone remote repository: %s",
            e.getMessage()), e);
      }

      LOG.info("Pushing namespace configs for : {}", pushNsRequest.getNamespaceId().getEntityName());

      //TODO: CDAP-20371, Add retry logic here in case the head at remote moved while we are doing push
      return writeNamespaceConfigAndPush(
          repositoryManager,
          pushNsRequest.getNamespaceConfig(),
          pushNsRequest.getCommitDetails()
      );
    }
  }


  @Override
  public PushAppResponse push(PushAppOperationRequest pushAppOperationRequest) throws NoChangesToPushException,
    AuthenticationConfigException {
    try (
      RepositoryManager repositoryManager = repoManagerFactory.create(pushAppOperationRequest.getNamespaceId(),
                                                                      pushAppOperationRequest.getRepositoryConfig())
    ) {
      try {
        repositoryManager.cloneRemote();
      } catch (GitAPIException | IOException e) {
        throw new GitOperationException(String.format("Failed to clone remote repository: %s",
                                                       e.getMessage()), e);
      }

      LOG.info("Pushing application configs for : {}", pushAppOperationRequest.getApp().getName());

      //TODO: CDAP-20371, Add retry logic here in case the head at remote moved while we are doing push
      return writeAppDetailAndPush(
        repositoryManager,
        pushAppOperationRequest.getApp(),
        pushAppOperationRequest.getCommitDetails()
      );
    }
  }

  @Override
  public PullAppResponse<?> pull(PulAppOperationRequest pulAppOperationRequest)
    throws NotFoundException, AuthenticationConfigException {
    String applicationName = pulAppOperationRequest.getApp().getApplication();
    String configFileName = generateConfigFileName(applicationName);
    LOG.info("Cloning remote to pull application {}", applicationName);
    Path appRelativePath = null;
    try (RepositoryManager repositoryManager =
           repoManagerFactory.create(pulAppOperationRequest.getApp().getNamespaceId(),
                                     pulAppOperationRequest.getRepositoryConfig())) {
      appRelativePath = repositoryManager.getFileRelativePath(configFileName);
      String commitId = repositoryManager.cloneRemote();
      Path filePathToRead = validateAppConfigRelativePath(repositoryManager, appRelativePath);
      if (!Files.exists(filePathToRead)) {
        throw new NotFoundException(String.format("App with name %s not found at path %s in git repository",
                                                  applicationName,
                                                  appRelativePath));
      }
      LOG.info("Getting file hash for application {}", applicationName);
      String fileHash = repositoryManager.getFileHash(appRelativePath, commitId);
      String contents = new String(Files.readAllBytes(filePathToRead), StandardCharsets.UTF_8);
      AppRequest<?> appRequest = DECODE_GSON.fromJson(contents, AppRequest.class);
      ApplicationDetail detail = DECODE_GSON.fromJson(contents, ApplicationDetail.class);
      return new PullAppResponse<>(applicationName, fileHash, appRequest, detail.getPreferences(), detail.getSchedules());
    } catch (GitAPIException e) {
      throw new GitOperationException(String.format("Failed to pull application %s: %s",
                                                     applicationName,
                                                     e.getMessage()), e);
    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException(String.format(
        "Failed to de-serialize application json at path %s. Ensure application json is valid.",
        appRelativePath), e);
    } catch (NotFoundException | AuthenticationConfigException | IllegalArgumentException e) {
      // TODO(CDAP-20410): Create exception classes that derive from a common class instead of propagating.
      throw e;
    } catch (Exception e) {
      throw new SourceControlException(String.format("Failed to pull application %s.", applicationName), e);
    }
  }

  /**
   * Atomic operation of writing namespace config and push, return the push response.
   *
   * @param repositoryManager {@link RepositoryManager} to conduct git operations
   * @param nsToPush         {@link NamespaceConfigData} to push
   * @param commitDetails     {@link CommitMeta} from user input
   * @return {@link PushNamespaceConfigResponse}
   * @throws NoChangesToPushException if there's no change between the application in namespace and git repository
   * @throws SourceControlException   for failures while writing config file or doing git operations
   */
  private PushNamespaceConfigResponse writeNamespaceConfigAndPush(
      RepositoryManager repositoryManager,
      NamespaceConfigData nsToPush,
      CommitMeta commitDetails)
      throws NoChangesToPushException {
    try {
      // Creates the base directory if it does not exist. This method does not throw an exception if the directory
      // already exists. This is for the case that the repo is new and user configured prefix path.
      Files.createDirectories(repositoryManager.getBasePath());
    } catch (IOException e) {
      throw new SourceControlException("Failed to create repository base directory", e);
    }

    String configFileName = generateNamepaceConfigFileName(nsToPush.getName());

    Path appRelativePath = repositoryManager.getFileRelativePath(configFileName);
    Path filePathToWrite;
    try {
      filePathToWrite = validateAppConfigRelativePath(repositoryManager, appRelativePath);
    } catch (IllegalArgumentException e) {
      throw new SourceControlException(String.format("Failed to push namespace %s: %s",
          nsToPush.getName(),
          e.getMessage()), e);
    }
    // Opens the file for writing, creating the file if it doesn't exist,
    // or truncating an existing regular-file to a size of 0
    try (FileWriter writer = new FileWriter(filePathToWrite.toString())) {
      GSON.toJson(nsToPush, writer);
    } catch (IOException e) {
      throw new ConfigFileWriteException(
          String.format("Failed to write namespace config to path %s", appRelativePath), e
      );
    }

    LOG.debug("Wrote namespace configs for {} in file {}", nsToPush.getName(), appRelativePath);

    try {
      // TODO: CDAP-20383, handle NoChangesToPushException
      //  Define the case that the application to push does not have any changes
      String gitFileHash = repositoryManager.commitAndPush(commitDetails, appRelativePath);
      return new PushNamespaceConfigResponse(nsToPush.getName(), gitFileHash);
    } catch (GitAPIException e) {
      throw new GitOperationException(String.format("Failed to push config to git: %s", e.getMessage()), e);
    }
  }

  /**
   * Atomic operation of writing application and push, return the push response.
   *
   * @param repositoryManager {@link RepositoryManager} to conduct git operations
   * @param appToPush         {@link ApplicationDetail} to push
   * @param commitDetails     {@link CommitMeta} from user input
   * @return {@link PushAppResponse}
   * @throws NoChangesToPushException if there's no change between the application in namespace and git repository
   * @throws SourceControlException   for failures while writing config file or doing git operations
   */
  private PushAppResponse writeAppDetailAndPush(RepositoryManager repositoryManager,
                                                ApplicationDetail appToPush,
                                                CommitMeta commitDetails)
    throws NoChangesToPushException {
    try {
      // Creates the base directory if it does not exist. This method does not throw an exception if the directory
      // already exists. This is for the case that the repo is new and user configured prefix path.
      Files.createDirectories(repositoryManager.getBasePath());
    } catch (IOException e) {
      throw new SourceControlException("Failed to create repository base directory", e);
    }

    String configFileName = generateConfigFileName(appToPush.getName());

    Path appRelativePath = repositoryManager.getFileRelativePath(configFileName);
    Path filePathToWrite;
    try {
      filePathToWrite = validateAppConfigRelativePath(repositoryManager, appRelativePath);
    } catch (IllegalArgumentException e) {
      throw new SourceControlException(String.format("Failed to push application %s: %s",
                                                     appToPush.getName(),
                                                     e.getMessage()), e);
    }
    // Opens the file for writing, creating the file if it doesn't exist,
    // or truncating an existing regular-file to a size of 0
    try (FileWriter writer = new FileWriter(filePathToWrite.toString())) {
      GSON.toJson(appToPush, writer);
    } catch (IOException e) {
      throw new ConfigFileWriteException(
          String.format("Failed to write application config to path %s", appRelativePath), e
      );
    }

    LOG.debug("Wrote application configs for {} in file {}", appToPush.getName(), appRelativePath);

    try {
      // TODO: CDAP-20383, handle NoChangesToPushException
      //  Define the case that the application to push does not have any changes
      String gitFileHash = repositoryManager.commitAndPush(commitDetails, appRelativePath);
      return new PushAppResponse(appToPush.getName(), appToPush.getAppVersion(), gitFileHash);
    } catch (GitAPIException e) {
      throw new GitOperationException(String.format("Failed to push config to git: %s", e.getMessage()), e);
    }
  }

  /**
   * Generate config file name from app name.
   * Currently, it only adds `.json` as extension with app name being the filename.
   *
   * @param appName Name of the application
   * @return The file name we want to store application config in
   */
  private String generateConfigFileName(String appName) {
    return String.format("%s.json", appName);
  }

  /**
   * Generate config file name from namespace name.
   * Currently, it only adds `namespace.json` as extension with namespace name being the filename.
   *
   * @param nsName Name of the namespace
   * @return The file name we want to store application config in
   */
  private String generateNamepaceConfigFileName(String nsName) {
    return String.format("%s.namespace.json", nsName);
  }

  /**
   * Validates if the resolved path is not a symbolic link and not a directory.
   *
   * @param repositoryManager the RepositoryManager
   * @param appRelativePath   the relative {@link Path} of the application to write to
   * @return A valid application config file relative path
   */
  private Path validateAppConfigRelativePath(RepositoryManager repositoryManager, Path appRelativePath) throws
    IllegalArgumentException {
    Path filePath = repositoryManager.getRepositoryRoot().resolve(appRelativePath);
    if (Files.isSymbolicLink(filePath)) {
      throw new IllegalArgumentException(String.format(
        "%s exists but refers to a symbolic link. Symbolic links are " + "not allowed.", appRelativePath));
    }
    if (Files.isDirectory(filePath)) {
      throw new IllegalArgumentException(String.format("%s refers to a directory not a file.", appRelativePath));
    }
    return filePath;
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

