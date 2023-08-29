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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.CommonProgramClassLoader;
import io.cdap.cdap.common.app.ReadonlyArtifactRepositoryAccessor;
import io.cdap.cdap.common.app.DeploymentDryrun;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.AppFabric;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.lang.DirectoryClassLoader;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.lang.jar.ClassLoaderFolder;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.FileUtils;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.DeploymentDryrunResult;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDescriptor;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.PullAppDryrunResponse;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.ConfigFileWriteException;
import io.cdap.cdap.sourcecontrol.GitOperationException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.RepositoryManagerFactory;
import io.cdap.cdap.sourcecontrol.SecureSystemReader;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.twill.filesystem.Location;
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
  private final Impersonator impersonator;
  private final CConfiguration cConf;
  private final File tmpDir;

  @Inject
  InMemorySourceControlOperationRunner(RepositoryManagerFactory repoManagerFactory, Impersonator impersonator, CConfiguration cConf) {
    this.repoManagerFactory = repoManagerFactory;
    this.impersonator = impersonator;
    this.cConf = cConf;
    this.tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
        cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
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
      return new PullAppResponse<>(applicationName, fileHash, appRequest);
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

  @Override
  public PullAppDryrunResponse pullAndDryrun(
      PullAndDryrunAppOperationRequest pullAndDryrunAppOperationRequest,
      ReadonlyArtifactRepositoryAccessor artifactRepository) throws Exception {

    String applicationName = pullAndDryrunAppOperationRequest.getApp().getApplication();
    ApplicationId appId = pullAndDryrunAppOperationRequest.getAppId();
    String configFileName = generateConfigFileName(applicationName);
    LOG.info("Cloning remote to pull application {}", applicationName);
    Path appRelativePath = null;
    try (RepositoryManager repositoryManager =
        repoManagerFactory.create(pullAndDryrunAppOperationRequest.getApp().getNamespaceId(),
            pullAndDryrunAppOperationRequest.getRepositoryConfig())) {
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

      DeploymentDryrun dryrun = new DeploymentDryrun(applicationName, fileHash, appId, appRequest, artifactRepository) {
        @Override
        protected CloseableClassLoader createArtifactClassLoader(
            ArtifactDescriptor artifactDescriptor, ArtifactId artifactId) throws IOException {
          return createCloasableClassloader(artifactDescriptor, artifactId);
        }
      };
      DeploymentDryrunResult result = dryrun.dryrun();

      return new PullAppDryrunResponse(applicationName, fileHash, result);
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

  public List<PullAppDryrunResponse> pullAndDryrunMulti(
      List<PullAndDryrunAppOperationRequest> pullAndDryrunAppOperationRequests,
      NamespaceId namespace,
      RepositoryConfig repoConfig,
      ReadonlyArtifactRepositoryAccessor artifactRepository
  ) throws Exception {
    // TODO Better error handling
    List<PullAppDryrunResponse> responses = new ArrayList<>();
    List<DeploymentDryrun> dryruns = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(AppFabric.DEFAULT_WORKER_THREADS);

    try (RepositoryManager repositoryManager =
        repoManagerFactory.create(namespace, repoConfig)) {

      String commitId = repositoryManager.cloneRemote();

      for (PullAndDryrunAppOperationRequest pullRequest: pullAndDryrunAppOperationRequests) {
        String applicationName = pullRequest.getApp().getApplication();
        ApplicationId appId = pullRequest.getAppId();
        String configFileName = generateConfigFileName(applicationName);
        LOG.info("Cloning remote to pull application {}", applicationName);

        Path appRelativePath = repositoryManager.getFileRelativePath(configFileName);
        Path filePathToRead = validateAppConfigRelativePath(repositoryManager, appRelativePath);
        if (!Files.exists(filePathToRead)) {
          throw new NotFoundException(
              String.format("App with name %s not found at path %s in git repository",
                  applicationName,
                  appRelativePath));
        }

        LOG.info("Getting file hash for application {}", applicationName);
        String fileHash = repositoryManager.getFileHash(appRelativePath, commitId);
        String contents = new String(Files.readAllBytes(filePathToRead), StandardCharsets.UTF_8);
        AppRequest<?> appRequest = DECODE_GSON.fromJson(contents, AppRequest.class);

        DeploymentDryrun dryrun = new DeploymentDryrun(applicationName, fileHash, appId, appRequest, artifactRepository) {
          @Override
          protected CloseableClassLoader createArtifactClassLoader(
              ArtifactDescriptor artifactDescriptor, ArtifactId artifactId) throws IOException {
            return createCloasableClassloader(artifactDescriptor, artifactId);
          }
        };
        dryruns.add(dryrun);
      }

      for(DeploymentDryrun dryrun: dryruns) {
        executor.execute(dryrun);
      }
      shutdownAndAwaitTermination(executor);
      for(DeploymentDryrun dryrun: dryruns) {
        responses.add(new PullAppDryrunResponse(
            dryrun.getApplicationName(),
            dryrun.getFileHash(),
            dryrun.getResult()
        ));
      }

      return responses;
    } catch (GitAPIException e) {
      throw new GitOperationException(String.format("Failed to pull applications: %s",
          e.getMessage()), e);
    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException(String.format(
          "Failed to de-serialize application json. Ensure application json is valid: %s",
          e.getMessage()), e);
    } catch (NotFoundException | AuthenticationConfigException | IllegalArgumentException e) {
      // TODO(CDAP-20410): Create exception classes that derive from a common class instead of propagating.
      throw e;
    } catch (Exception e) {
      throw new SourceControlException("Failed to pull applications.", e);
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

  private CloseableClassLoader createCloasableClassloader(ArtifactDescriptor artifactDescriptor,
      io.cdap.cdap.proto.id.ArtifactId artifactId)
      throws IOException {
    EntityImpersonator classLoaderImpersonator = new EntityImpersonator(artifactId,
        impersonator);

    Iterator<Location> artifactLocations = ImmutableList.of(artifactDescriptor.getLocation()).iterator();
    return createClassLoader(artifactLocations, classLoaderImpersonator);
  }

  private CloseableClassLoader createClassLoader(Iterator<Location> artifactLocations,
      EntityImpersonator entityImpersonator) {

    if (!artifactLocations.hasNext()) {
      throw new IllegalArgumentException("Cannot create a classloader without an artifact.");
    }

    Location artifactLocation = artifactLocations.next();
    if (!artifactLocations.hasNext()) {
      return createClassLoader(artifactLocation, entityImpersonator);
    }

    try {
      ClassLoaderFolder classLoaderFolder = entityImpersonator.impersonate(
          () -> BundleJarUtil.prepareClassLoaderFolder(artifactLocation,
              () -> DirUtils.createTempDir(tmpDir)));

      CloseableClassLoader parentClassLoader = createClassLoader(artifactLocations,
          entityImpersonator);
      return new CloseableClassLoader(new DirectoryClassLoader(classLoaderFolder.getDir(),
          parentClassLoader, "lib"), () -> {
        Closeables.closeQuietly(parentClassLoader);
        Closeables.closeQuietly(classLoaderFolder);
      });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private CloseableClassLoader createClassLoader(Location artifactLocation,
      EntityImpersonator entityImpersonator) {
    try {
      ClassLoaderFolder classLoaderFolder = entityImpersonator.impersonate(
          () -> BundleJarUtil.prepareClassLoaderFolder(artifactLocation,
              () -> DirUtils.createTempDir(tmpDir)));

      CloseableClassLoader classLoader = createClassLoader(classLoaderFolder.getDir());
      return new CloseableClassLoader(classLoader, () -> {
        Closeables.closeQuietly(classLoader);
        Closeables.closeQuietly(classLoaderFolder);
      });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private CloseableClassLoader createClassLoader(File unpackDir) {
    CommonProgramClassLoader programClassLoader = new CommonProgramClassLoader(cConf, unpackDir,
          FilterClassLoader.create(getClass().getClassLoader()));
    final ClassLoader finalProgramClassLoader = programClassLoader;
    return new CloseableClassLoader(programClassLoader, () -> {
      if (finalProgramClassLoader instanceof Closeable) {
        Closeables.closeQuietly((Closeable) finalProgramClassLoader);
      }
    });
  }

  static void shutdownAndAwaitTermination(ExecutorService pool) {
    pool.shutdown();
    try {
      if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
        pool.shutdownNow();
        if (!pool.awaitTermination(60, TimeUnit.SECONDS))
          LOG.error("Pool did not terminate");
      }
    } catch (InterruptedException ex) {
      pool.shutdownNow();
      Thread.currentThread().interrupt();
    }
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

