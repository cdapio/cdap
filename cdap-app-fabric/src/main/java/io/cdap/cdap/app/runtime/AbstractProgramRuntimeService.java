/*
 * Copyright © 2014-2020 Cask Data, Inc.
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
package io.cdap.cdap.app.runtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.program.Programs;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.lang.jar.ClassLoaderFolder;
import io.cdap.cdap.common.twill.TwillAppNames;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentRuntimeInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.AppSpecInfo;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.service.SimpleRuntimeInfo;
import io.cdap.cdap.proto.InMemoryProgramLiveInfo;
import io.cdap.cdap.proto.NotRunningProgramLiveInfo;
import io.cdap.cdap.proto.ProgramLiveInfo;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;

/**
 * A ProgramRuntimeService that keeps an in memory map for all running programs.
 */
public abstract class AbstractProgramRuntimeService extends AbstractIdleService implements ProgramRuntimeService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramRuntimeService.class);
  private static final String CLUSTER_SCOPE = "cluster";
  private static final String APPLICATION_SCOPE = "app";
  private static final EnumSet<ProgramController.State> COMPLETED_STATES = EnumSet.of(ProgramController.State.COMPLETED,
                                                                                      ProgramController.State.KILLED,
                                                                                      ProgramController.State.ERROR);
  private final CConfiguration cConf;
  private final ReadWriteLock runtimeInfosLock;
  private final Table<ProgramType, RunId, RuntimeInfo> runtimeInfos;
  private final ProgramRunnerFactory programRunnerFactory;
  private final ProgramStateWriter programStateWriter;
  private final ConfiguratorFactory configuratorFactory;
  private ArtifactRepository noAuthArtifactRepository;
  private ProgramRunnerFactory remoteProgramRunnerFactory;
  private TwillRunnerService remoteTwillRunnerService;
  private ExecutorService executor;
  private final LocationFactory locationFactory;
  private final RemoteClientFactory remoteClientFactory;

  protected AbstractProgramRuntimeService(CConfiguration cConf,
                                          ProgramRunnerFactory programRunnerFactory,
                                          ArtifactRepository noAuthArtifactRepository,
                                          ProgramStateWriter programStateWriter,
                                          ConfiguratorFactory configuratorFactory,
                                          LocationFactory locationFactory,
                                          RemoteClientFactory remoteClientFactory) {
    this.cConf = cConf;
    this.runtimeInfosLock = new ReentrantReadWriteLock();
    this.runtimeInfos = HashBasedTable.create();
    this.programRunnerFactory = programRunnerFactory;
    this.noAuthArtifactRepository = noAuthArtifactRepository;
    this.programStateWriter = programStateWriter;
    this.configuratorFactory = configuratorFactory;
    this.locationFactory = locationFactory;
    this.remoteClientFactory = remoteClientFactory;
  }

  /**
   * Optional guice injection for the {@link ProgramRunnerFactory} used for remote execution. It is optional because
   * in unit-test we don't have need for that.
   */
  @Inject(optional = true)
  void setRemoteProgramRunnerFactory(@Constants.AppFabric.RemoteExecution ProgramRunnerFactory runnerFactory) {
    this.remoteProgramRunnerFactory = runnerFactory;
  }

  /**
   * Optional guice injection for the {@link TwillRunnerService} used for remote execution. It is optional because
   * in unit-test we don't have need for that.
   */
  @Inject(optional = true)
  void setRemoteTwillRunnerService(@Constants.AppFabric.RemoteExecution TwillRunnerService twillRunnerService) {
    this.remoteTwillRunnerService = twillRunnerService;
  }

  @Override
  public final RuntimeInfo run(ProgramDescriptor programDescriptor, ProgramOptions options, RunId runId) {
    ProgramId programId = programDescriptor.getProgramId();
    ProgramRunId programRunId = programId.run(runId);
    ClusterMode clusterMode = ProgramRunners.getClusterMode(options);

    // Creates the ProgramRunner based on the cluster mode
    ProgramRunnerFactory progRunnerFactory = (clusterMode == ClusterMode.ON_PREMISE) ? programRunnerFactory :
      Optional.ofNullable(remoteProgramRunnerFactory).orElseThrow(UnsupportedOperationException::new);
    ProgramRunner runner = progRunnerFactory.create(programId.getType());

    File tempDir = createTempDirectory(programId, runId);
    AtomicReference<Runnable> cleanUpTaskRef = new AtomicReference<>(createCleanupTask(tempDir, runner));
    DelayedProgramController controller = new DelayedProgramController(programRunId);
    RuntimeInfo runtimeInfo = createRuntimeInfo(controller, programId, () -> cleanUpTaskRef.get().run());
    updateRuntimeInfo(runtimeInfo);

    String peer = options.getArguments().getOption(ProgramOptionConstants.PEER_NAME);
    if (peer != null) {
      try {
        // For tethered pipeline runs, fetch artifacts from ArtifactCacheService
        String basePath = String.format("%s/peers/%s", Constants.Gateway.INTERNAL_API_VERSION_3, peer);
        RemoteClient remoteClient = remoteClientFactory.createRemoteClient(
          Constants.Service.ARTIFACT_CACHE_SERVICE,
          RemoteClientFactory.NO_VERIFY_HTTP_REQUEST_CONFIG,
          basePath);
        RemoteArtifactRepositoryReader artifactRepositoryReader = new RemoteArtifactRepositoryReader(
          locationFactory, remoteClient);
        noAuthArtifactRepository = new RemoteArtifactRepository(cConf, artifactRepositoryReader,
                                                                progRunnerFactory);
      } catch (Exception e) {
        controller.failed(e);
        programStateWriter.error(programRunId, e);
        LOG.error("Exception while trying to create remote client factory", e);
        return runtimeInfo;
      }
    }

    executor.execute(() -> {
      try {
        // Get the artifact details and save it into the program options.
        ArtifactId artifactId = programDescriptor.getArtifactId();
        ArtifactDetail artifactDetail = getArtifactDetail(artifactId);
        ApplicationSpecification appSpec = programDescriptor.getApplicationSpecification();
        ProgramDescriptor newProgramDescriptor = programDescriptor;

        boolean isPreview = Boolean.valueOf(
          options.getArguments().getOption(ProgramOptionConstants.IS_PREVIEW, "false"));
        // do the app spec regeneration if the mode is on premise, for isolated mode, the regeneration is done on the
        // runtime environment before the program launch
        // for preview we already have a resolved app spec, so no need to regenerate the app spec again
        if (!isPreview && appSpec != null && ClusterMode.ON_PREMISE.equals(clusterMode)) {
          try {
            ApplicationSpecification generatedAppSpec =
              regenerateAppSpec(artifactDetail, programId, artifactId, appSpec, options);
            appSpec = generatedAppSpec != null ? generatedAppSpec : appSpec;
            newProgramDescriptor = new ProgramDescriptor(programDescriptor.getProgramId(), appSpec);
          } catch (Exception e) {
            LOG.warn("Failed to regenerate the app spec for program {}, using the existing app spec", programId);
          }
        }

        ProgramOptions runtimeProgramOptions = updateProgramOptions(
          artifactId, programId, options, runId, clusterMode,
          Iterables.getFirst(artifactDetail.getMeta().getClasses().getApps(), null));

        // Take a snapshot of all the plugin artifacts used by the program
        ProgramOptions optionsWithPlugins = createPluginSnapshot(runtimeProgramOptions, programId, tempDir,
                                                                 newProgramDescriptor.getApplicationSpecification());

        // Create and run the program
        Program executableProgram = createProgram(cConf, runner, newProgramDescriptor, artifactDetail, tempDir);
        cleanUpTaskRef.set(createCleanupTask(cleanUpTaskRef.get(), executableProgram));

        controller.setProgramController(runner.run(executableProgram, optionsWithPlugins));
      } catch (Exception e) {
        controller.failed(e);
        programStateWriter.error(programRunId, e);
        LOG.error("Exception while trying to run program", e);
      }
    });
    return runtimeInfo;
  }

  @Override
  public ProgramLiveInfo getLiveInfo(ProgramId programId) {
    return isRunning(programId) ? new InMemoryProgramLiveInfo(programId)
      : new NotRunningProgramLiveInfo(programId);
  }

  protected ArtifactDetail getArtifactDetail(ArtifactId artifactId) throws Exception {
    return noAuthArtifactRepository.getArtifact(Id.Artifact.fromEntityId(artifactId));
  }

  /**
   * Creates a {@link Program} for the given {@link ProgramRunner} from the given program jar {@link Location}.
   */
  protected Program createProgram(CConfiguration cConf, ProgramRunner programRunner,
                                  ProgramDescriptor programDescriptor,
                                  ArtifactDetail artifactDetail, final File tempDir) throws IOException {

    Location programJarLocation = artifactDetail.getDescriptor().getLocation();
    ClassLoaderFolder classLoaderFolder;
    try {
      // If the program jar is not a directory, take a snapshot of the jar file to avoid mutation.
      if (!programJarLocation.isDirectory()) {
        File targetFile = new File(tempDir, "program.jar");
        try {
          programJarLocation = Locations.toLocation(Locations.linkOrCopyOverwrite(programJarLocation,
                                                                         targetFile));
        } catch (FileAlreadyExistsException ex) {
          LOG.warn("Program file {} already exists and can not be replaced.", targetFile.getAbsolutePath());
        }
    }
      // Unpack the JAR file
      classLoaderFolder = BundleJarUtil.prepareClassLoaderFolder(
        programJarLocation, () -> Files.createTempDirectory(tempDir.toPath(), "unpacked").toFile());
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      // should not happen
      throw Throwables.propagate(e);
    }
    return Programs.create(cConf, programRunner, programDescriptor, programJarLocation, classLoaderFolder.getDir());
  }

  private Runnable createCleanupTask(final Object... resources) {
    AtomicBoolean executed = new AtomicBoolean();
    return () -> {
      if (!executed.compareAndSet(false, true)) {
        return;
      }
      List<Object> resourceList = new ArrayList<>(Arrays.asList(resources));
      Collections.reverse(resourceList);
      for (Object resource : resourceList) {
        if (resource == null) {
          continue;
        }

        try {
          if (resource instanceof File) {
            File file = (File) resource;
            if (file.isDirectory()) {
              DirUtils.deleteDirectoryContents(file);
            } else {
              file.delete();
            }
          } else if (resource instanceof Closeable) {
            Closeables.closeQuietly((Closeable) resource);
          } else if (resource instanceof Runnable) {
            ((Runnable) resource).run();
          }
        } catch (Throwable t) {
          LOG.warn("Exception when cleaning up resource {}", resource, t);
        }
      }
    };
  }

  /**
   * Creates a local temporary directory for this program run.
   */
  private File createTempDirectory(ProgramId programId, RunId runId) {
    File tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                            cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    File dir = new File(tempDir, String.format("%s.%s.%s.%s.%s",
                                               programId.getType().name().toLowerCase(),
                                               programId.getNamespace(), programId.getApplication(),
                                               programId.getProgram(), runId.getId()));
    dir.mkdirs();
    return dir;
  }

  /**
   * Regenerates the app spec before the program start
   *
   * @return the regenerated app spec, or null if there is any exception generating the app spec.
   */
  @Nullable
  private ApplicationSpecification regenerateAppSpec(
    ArtifactDetail artifactDetail, ProgramId programId, ArtifactId artifactId,
    ApplicationSpecification existingAppSpec,
    ProgramOptions options) throws InterruptedException, ExecutionException, TimeoutException {
    ApplicationClass appClass = Iterables.getFirst(artifactDetail.getMeta().getClasses().getApps(), null);
    if (appClass == null) {
      // This should never happen.
      throw new IllegalStateException(String.format(
        "No application class found in artifact '%s' in namespace '%s'.",
        artifactDetail.getDescriptor().getArtifactId(), programId.getNamespace()));
    }

    AppDeploymentInfo deploymentInfo = new AppDeploymentInfo(
      artifactId, artifactDetail.getDescriptor().getLocation(), programId.getNamespaceId(), appClass,
      existingAppSpec.getName(), existingAppSpec.getAppVersion(), existingAppSpec.getConfiguration(), null, false,
      new AppDeploymentRuntimeInfo(existingAppSpec, options.getUserArguments().asMap(),
                                   options.getArguments().asMap()));
    Configurator configurator = this.configuratorFactory.create(deploymentInfo);
    ListenableFuture<ConfigResponse> future = configurator.config();
    ConfigResponse response = future.get(120, TimeUnit.SECONDS);
    
    if (response.getExitCode() == 0) {
      AppSpecInfo appSpecInfo = response.getAppSpecInfo();
      if (appSpecInfo != null && appSpecInfo.getAppSpec() != null) {
        return appSpecInfo.getAppSpec();
      }
    }
    return null;
  }

  /**
   * Return the copy of the {@link ProgramOptions} including locations of plugin artifacts in it.
   * @param options the {@link ProgramOptions} in which the locations of plugin artifacts needs to be included
   * @param programId Id of the Program
   * @param tempDir Temporary Directory to create the plugin artifact snapshot
   * @param appSpec program's Application Specification
   * @return the copy of the program options with locations of plugin artifacts included in them
   */
  private ProgramOptions createPluginSnapshot(ProgramOptions options, ProgramId programId, File tempDir,
                                              @Nullable ApplicationSpecification appSpec) throws Exception {
    // appSpec is null in an unit test
    if (appSpec == null) {
      return options;
    }

    Set<String> files = Sets.newHashSet();
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(options.getArguments().asMap());
    for (Map.Entry<String, Plugin> pluginEntry : appSpec.getPlugins().entrySet()) {
      Plugin plugin = pluginEntry.getValue();
      File destFile = new File(tempDir, Artifacts.getFileName(plugin.getArtifactId()));
      // Skip if the file has already been copied.
      if (!files.add(destFile.getName())) {
        continue;
      }

      try {
        ArtifactId artifactId = Artifacts.toProtoArtifactId(programId.getNamespaceId(), plugin.getArtifactId());
        String peer = options.getArguments().getOption(ProgramOptionConstants.PEER_NAME);
        // Copy artifact from ArtifactCacheService if this pipeline run is for a tethered peer. Else copy artifact
        // from the filesystem.
        if (peer != null) {
          try (InputStream in = noAuthArtifactRepository.newInputStream(Id.Artifact.fromEntityId(artifactId))) {
            copyArtifact(artifactId, in, destFile);
          }
        } else {
          copyArtifact(artifactId, noAuthArtifactRepository.getArtifact(Id.Artifact.fromEntityId(artifactId)),
                       destFile);
        }
      } catch (ArtifactNotFoundException e) {
        throw new IllegalArgumentException(String.format("Artifact %s could not be found", plugin.getArtifactId()), e);
      }
    }
    LOG.debug("Plugin artifacts of {} copied to {}", programId, tempDir.getAbsolutePath());
    builder.put(ProgramOptionConstants.PLUGIN_DIR, tempDir.getAbsolutePath());
    return new SimpleProgramOptions(options.getProgramId(), new BasicArguments(builder.build()),
                                    options.getUserArguments(), options.isDebug());
  }

  /**
   * Copies the artifact jar to the given target file.
   *
   * @param artifactId artifact id of the artifact to be copied
   * @param artifactDetail detail information of the artifact to be copied
   * @param targetFile target file to copy to
   * @throws IOException if the copying failed
   */
  protected void copyArtifact(ArtifactId artifactId,
                              ArtifactDetail artifactDetail, File targetFile) throws IOException {
    Locations.linkOrCopy(artifactDetail.getDescriptor().getLocation(), targetFile);
  }

  /**
   * Copies the artifact jar to the given target file.
   *
   * @param artifactId artifact id of the artifact to be copied
   * @param in input stream that the artifact should be copied from
   * @param targetFile target file to copy to
   * @throws IOException if the copying failed
   */
  protected void copyArtifact(ArtifactId artifactId, InputStream in, File targetFile) throws IOException {
    Files.copy(in, targetFile.toPath());
  }

  protected Map<String, String> getExtraProgramOptions() {
    return Collections.emptyMap();
  }

  /**
   * Updates the given {@link ProgramOptions} and return a new instance.
   * It copies the {@link ProgramOptions}. Then it adds all entries returned by {@link #getExtraProgramOptions()}
   * followed by adding the {@link RunId} to the system arguments.
   *
   * Also scope resolution will be performed on the user arguments on the application and program.
   *
   * @param programId the program id
   * @param options The {@link ProgramOptions} in which the RunId to be included
   * @param runId   The RunId to be included
   * @param clusterMode clustermode for the program run
   * @param applicationClass application class for the program
   * @return the copy of the program options with RunId included in them
   */
  private ProgramOptions updateProgramOptions(ArtifactId artifactId, ProgramId programId,
                                              ProgramOptions options, RunId runId, ClusterMode clusterMode,
                                              ApplicationClass applicationClass) {
    // Build the system arguments
    Map<String, String> systemArguments = new HashMap<>(options.getArguments().asMap());
    // don't add these system arguments if they're already there
    // this can happen if this is a program within a workflow, and the workflow already added these arguments
    for (Map.Entry<String, String> extraOption : getExtraProgramOptions().entrySet()) {
      systemArguments.putIfAbsent(extraOption.getKey(), extraOption.getValue());
    }
    systemArguments.putIfAbsent(ProgramOptionConstants.RUN_ID, runId.getId());
    systemArguments.putIfAbsent(ProgramOptionConstants.ARTIFACT_ID, Joiner.on(':').join(artifactId.toIdParts()));
    if (clusterMode == ClusterMode.ISOLATED) {
      systemArguments.putIfAbsent(ProgramOptionConstants.APPLICATION_CLASS, applicationClass.getClassName());
    }

    // Resolves the user arguments
    // First resolves at the cluster scope if the cluster.name is not empty
    String clusterName = options.getArguments().getOption(Constants.CLUSTER_NAME);
    Map<String, String> userArguments = options.getUserArguments().asMap();
    if (!Strings.isNullOrEmpty(clusterName)) {
      userArguments = RuntimeArguments.extractScope(CLUSTER_SCOPE, clusterName, userArguments);
    }
    // Then resolves at the application scope
    userArguments = RuntimeArguments.extractScope(APPLICATION_SCOPE, programId.getApplication(), userArguments);
    // Then resolves at the program level
    userArguments = RuntimeArguments.extractScope(programId.getType().getScope(), programId.getProgram(),
                                                  userArguments);

    return new SimpleProgramOptions(options.getProgramId(), new BasicArguments(systemArguments),
                                    new BasicArguments(userArguments), options.isDebug());
  }

  protected RuntimeInfo createRuntimeInfo(ProgramController controller, ProgramId programId, Runnable cleanUpTask) {
    return new SimpleRuntimeInfo(controller, programId, cleanUpTask);
  }

  protected List<RuntimeInfo> getRuntimeInfos() {
    Lock lock = runtimeInfosLock.readLock();
    lock.lock();
    try {
      return ImmutableList.copyOf(runtimeInfos.values());
    } finally {
      lock.unlock();
    }
  }

  @Nullable
  @Override
  public RuntimeInfo lookup(ProgramId programId, RunId runId) {
    Lock lock = runtimeInfosLock.readLock();
    lock.lock();
    try {
      RuntimeInfo info = runtimeInfos.get(programId.getType(), runId);
      if (info != null || remoteTwillRunnerService == null) {
        return info;
      }
    } finally {
      lock.unlock();
    }

    // The remote twill runner uses the same runId
    return lookupFromTwillRunner(remoteTwillRunnerService, programId.run(runId.getId()), runId);
  }

  @Override
  public Map<RunId, RuntimeInfo> list(ProgramType type) {
    Map<RunId, RuntimeInfo> result = new HashMap<>();

    Lock lock = runtimeInfosLock.readLock();
    lock.lock();
    try {
      result.putAll(runtimeInfos.row(type));
    } finally {
      lock.unlock();
    }

    // Add any missing RuntimeInfo from the remote twill runner
    if (remoteTwillRunnerService == null) {
      return Collections.unmodifiableMap(result);
    }

    for (TwillRunner.LiveInfo liveInfo : remoteTwillRunnerService.lookupLive()) {
      ProgramId programId = TwillAppNames.fromTwillAppName(liveInfo.getApplicationName(), false);
      if (programId == null || !programId.getType().equals(type)) {
        continue;
      }

      for (TwillController controller : liveInfo.getControllers()) {
        // For remote twill runner, the twill run id and cdap run id are the same
        RunId runId = controller.getRunId();
        if (result.computeIfAbsent(runId, rid -> createRuntimeInfo(programId, runId, controller)) == null) {
          LOG.warn("Unable to create runtime info for program {} with run id {}", programId, runId);
        }
      }
    }

    return Collections.unmodifiableMap(result);
  }

  @Override
  public Map<RunId, RuntimeInfo> list(final ProgramId program) {
    return Maps.filterValues(list(program.getType()), info -> info.getProgramId().equals(program));
  }

  @Override
  public List<RuntimeInfo> listAll(ProgramType... types) {
    List<RuntimeInfo> runningPrograms = new ArrayList<>();
    for (ProgramType type : types) {
      for (Map.Entry<RunId, RuntimeInfo> entry : list(type).entrySet()) {
        ProgramController.State programState = entry.getValue().getController().getState();
        if (programState.isDone()) {
          continue;
        }
        runningPrograms.add(entry.getValue());
      }
    }
    return runningPrograms;
  }

  @Override
  protected void startUp() throws Exception {
    // Limits to at max poolSize number of concurrent program launch.
    // Also don't keep a thread around if it is idle for more than 60 seconds.
    int poolSize = cConf.getInt(Constants.AppFabric.PROGRAM_LAUNCH_THREADS);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(poolSize, poolSize, 60, TimeUnit.SECONDS,
                                                         new LinkedBlockingQueue<>(),
                                                         new ThreadFactoryBuilder()
                                                           .setNameFormat("program-start-%d").build());
    executor.allowCoreThreadTimeOut(true);
    this.executor = executor;
  }

  @Override
  protected void shutDown() throws Exception {
    if (executor != null) {
      executor.shutdown();
    }
  }

  /**
   * Uses the given {@link TwillRunner} to lookup {@link RuntimeInfo} for the given program run.
   *
   * @param twillRunner the {@link TwillRunner} to lookup from
   * @param programRunId the program run id
   * @param twillRunId the twill {@link RunId}
   * @return the corresponding {@link RuntimeInfo} or {@code null} if no runtime information was found
   */
  @Nullable
  protected RuntimeInfo lookupFromTwillRunner(TwillRunner twillRunner, ProgramRunId programRunId, RunId twillRunId) {
    TwillController twillController = twillRunner.lookup(TwillAppNames.toTwillAppName(programRunId.getParent()),
                                                         twillRunId);
    if (twillController == null) {
      return null;
    }

    return createRuntimeInfo(programRunId.getParent(), RunIds.fromString(programRunId.getRun()), twillController);
  }

  /**
   * Updates the runtime info cache by adding the given {@link RuntimeInfo} if it does not exist.
   *
   * @param info information about the running program
   */
  @VisibleForTesting
  void updateRuntimeInfo(RuntimeInfo info) {
    // Add the runtime info if it does not exist in the cache.
    Lock lock = runtimeInfosLock.writeLock();
    lock.lock();
    try {
      if (runtimeInfos.contains(info.getType(), info.getController().getRunId())) {

        LOG.debug("RuntimeInfo already exists: {}", info.getController().getProgramRunId());
        cleanupRuntimeInfo(info);
        return;
      }
      runtimeInfos.put(info.getType(), info.getController().getRunId(), info);
    } finally {
      lock.unlock();
    }

    LOG.debug("Added RuntimeInfo: {}", info.getController().getProgramRunId());

    ProgramController controller = info.getController();
    controller.addListener(new AbstractListener() {

      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        if (COMPLETED_STATES.contains(currentState)) {
          remove(info);
        }
      }

      @Override
      public void completed() {
        remove(info);
      }

      @Override
      public void killed() {
        remove(info);
      }

      @Override
      public void error(Throwable cause) {
        remove(info);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  private void remove(RuntimeInfo info) {
    RuntimeInfo removedInfo = null;
    Lock lock = runtimeInfosLock.writeLock();
    lock.lock();
    try {
      removedInfo = runtimeInfos.remove(info.getType(), info.getController().getRunId());
    } finally {
      lock.unlock();
      cleanupRuntimeInfo(removedInfo);
    }

    if (removedInfo != null) {
      LOG.debug("RuntimeInfo removed: {}", removedInfo.getController().getProgramRunId());
    }
  }

  /**
   * Returns {@code true} if there is any running instance of the given program.
   */
  protected boolean isRunning(ProgramId programId) {
    return !list(programId).isEmpty();
  }

  /**
   * Creates a {@link RuntimeInfo} representing the given program run.
   *
   * @param programId the program id for the program run
   * @param runId the run id for the program run
   * @param twillController the {@link TwillController} controlling the corresponding twill application
   * @return a {@link RuntimeInfo} or {@code null} if not able to create the {@link RuntimeInfo} due to unexpected
   *         and unrecoverable error/bug.
   */
  @Nullable
  protected RuntimeInfo createRuntimeInfo(ProgramId programId, RunId runId, TwillController twillController) {
    try {
      ProgramController controller = createController(programId, runId, twillController);
      RuntimeInfo runtimeInfo = controller == null ? null : createRuntimeInfo(controller, programId, () -> { });

      if (runtimeInfo != null) {
        updateRuntimeInfo(runtimeInfo);
      }
      return runtimeInfo;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Creates a {@link ProgramController} from the given {@link TwillController}.
   *
   * @param programId the program id for the program run
   * @param runId the run id for the program run
   * @param controller the {@link TwillController} controlling the corresponding twill application
   * @return a {@link ProgramController} or {@code null} if there is unexpected error/bug
   */
  @Nullable
  private ProgramController createController(ProgramId programId, RunId runId, TwillController controller) {
    ProgramRunnerFactory factory = runId.equals(controller.getRunId())
      ? remoteProgramRunnerFactory : programRunnerFactory;

    ProgramRunner programRunner;
    try {
      programRunner = factory.create(programId.getType());
    } catch (IllegalArgumentException e) {
      // This shouldn't happen. If it happen, it means CDAP was incorrectly install such that some of the program
      // type is not support (maybe due to version mismatch in upgrade).
      LOG.error("Unsupported program type {} for program {}. " +
                  "It is likely caused by incorrect CDAP installation or upgrade to incompatible CDAP version",
                programId.getType(), programId);
      return null;
    }

    if (!(programRunner instanceof ProgramControllerCreator)) {
      // This is also unexpected. If it happen, it means the CDAP core or the runtime provider extension was wrongly
      // implemented
      ResourceReport resourceReport = controller.getResourceReport();
      LOG.error("Unable to create ProgramController for program {} for twill application {}. It is likely caused by " +
                  "invalid CDAP program runtime extension.",
                programId, resourceReport == null ? "'unknown twill application'" : resourceReport.getApplicationId());
      return null;
    }

    return ((ProgramControllerCreator) programRunner).createProgramController(programId.run(runId), controller);
  }

  /**
   * Cleanup the given {@link RuntimeInfo} if it is {@link Closeable}.
   */
  private void cleanupRuntimeInfo(@Nullable RuntimeInfo info) {
    if (info instanceof Closeable) {
      Closeables.closeQuietly((Closeable) info);
    }
  }
}
