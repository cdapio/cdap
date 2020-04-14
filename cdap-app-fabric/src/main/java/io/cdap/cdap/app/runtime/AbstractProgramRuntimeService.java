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
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.program.Programs;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.twill.TwillAppNames;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
  private final ArtifactRepository noAuthArtifactRepository;
  private final ProgramStateWriter programStateWriter;
  private ProgramRunnerFactory remoteProgramRunnerFactory;
  private TwillRunnerService remoteTwillRunnerService;
  private ExecutorService executor;

  protected AbstractProgramRuntimeService(CConfiguration cConf,
                                          ProgramRunnerFactory programRunnerFactory,
                                          ArtifactRepository noAuthArtifactRepository,
                                          ProgramStateWriter programStateWriter) {
    this.cConf = cConf;
    this.runtimeInfosLock = new ReentrantReadWriteLock();
    this.runtimeInfos = HashBasedTable.create();
    this.programRunnerFactory = programRunnerFactory;
    this.noAuthArtifactRepository = noAuthArtifactRepository;
    this.programStateWriter = programStateWriter;
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
    ProgramRunner runner = (clusterMode == ClusterMode.ON_PREMISE
      ? programRunnerFactory
      : Optional.ofNullable(remoteProgramRunnerFactory).orElseThrow(UnsupportedOperationException::new)
    ).create(programId.getType());


    File tempDir = createTempDirectory(programId, runId);
    AtomicReference<Runnable> cleanUpTaskRef = new AtomicReference<>(createCleanupTask(tempDir, runner));
    DelayedProgramController controller = new DelayedProgramController(programRunId);
    RuntimeInfo runtimeInfo = createRuntimeInfo(controller, programId, () -> cleanUpTaskRef.get().run());
    monitorProgram(runtimeInfo, () -> cleanUpTaskRef.get().run());

    executor.execute(() -> {
      try {
        // Get the artifact details and save it into the program options.
        ArtifactId artifactId = programDescriptor.getArtifactId();
        ArtifactDetail artifactDetail = getArtifactDetail(artifactId);
        ProgramOptions runtimeProgramOptions = updateProgramOptions(artifactId, programId, options, runId);

        // Take a snapshot of all the plugin artifacts used by the program
        ProgramOptions optionsWithPlugins = createPluginSnapshot(runtimeProgramOptions, programId, tempDir,
                                                                 programDescriptor.getApplicationSpecification());

        // Create and run the program
        Program executableProgram = createProgram(cConf, runner, programDescriptor, artifactDetail, tempDir);
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

    final Location programJarLocation = artifactDetail.getDescriptor().getLocation();

    // Take a snapshot of the JAR file to avoid program mutation
    final File unpackedDir = new File(tempDir, "unpacked");
    unpackedDir.mkdirs();
    try {
      File programJar = Locations.linkOrCopy(programJarLocation, new File(tempDir, "program.jar"));
      // Unpack the JAR file
      BundleJarUtil.unJar(programJar, unpackedDir);
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      // should not happen
      throw Throwables.propagate(e);
    }
    return Programs.create(cConf, programRunner, programDescriptor, programJarLocation, unpackedDir);
  }

  private Runnable createCleanupTask(final Object... resources) {
    return () -> {
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
    if (appSpec == null || appSpec.getPlugins().isEmpty()) {
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
        copyArtifact(artifactId, noAuthArtifactRepository.getArtifact(Id.Artifact.fromEntityId(artifactId)), destFile);
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
   * @return the copy of the program options with RunId included in them
   */
  private ProgramOptions updateProgramOptions(ArtifactId artifactId, ProgramId programId,
                                              ProgramOptions options, RunId runId) {
    // Build the system arguments
    Map<String, String> systemArguments = new HashMap<>(options.getArguments().asMap());
    // don't add these system arguments if they're already there
    // this can happen if this is a program within a workflow, and the workflow already added these arguments
    for (Map.Entry<String, String> extraOption : getExtraProgramOptions().entrySet()) {
      systemArguments.putIfAbsent(extraOption.getKey(), extraOption.getValue());
    }
    systemArguments.putIfAbsent(ProgramOptionConstants.RUN_ID, runId.getId());
    systemArguments.putIfAbsent(ProgramOptionConstants.ARTIFACT_ID, Joiner.on(':').join(artifactId.toIdParts()));

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
    return new SimpleRuntimeInfo(controller, programId);
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
    return Maps.filterValues(list(program.getType()), new Predicate<RuntimeInfo>() {
      @Override
      public boolean apply(RuntimeInfo info) {
        return info.getProgramId().equals(program);
      }
    });
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
    executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("program-start-%d").build());
  }

  @Override
  protected void shutDown() throws Exception {
    if (executor != null) {
      executor.shutdown();
    }
  }

  @VisibleForTesting
  void updateRuntimeInfo(ProgramType type, RunId runId, RuntimeInfo runtimeInfo) {
    Lock lock = runtimeInfosLock.readLock();
    lock.lock();
    try {
      if (!runtimeInfos.contains(type, runId)) {
        monitorProgram(runtimeInfo, createCleanupTask());
      }
    } finally {
      lock.unlock();
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
   * Starts monitoring a running program.
   *
   * @param runtimeInfo information about the running program
   * @param cleanUpTask task to run when program finished
   */
  private void monitorProgram(final RuntimeInfo runtimeInfo, final Runnable cleanUpTask) {
    final ProgramController controller = runtimeInfo.getController();
    controller.addListener(new AbstractListener() {

      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        if (!COMPLETED_STATES.contains(currentState)) {
          add(runtimeInfo);
        } else {
          cleanUpTask.run();
        }
      }

      @Override
      public void completed() {
        remove(runtimeInfo, cleanUpTask);
      }

      @Override
      public void killed() {
        remove(runtimeInfo, cleanUpTask);
      }

      @Override
      public void error(Throwable cause) {
        remove(runtimeInfo, cleanUpTask);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  private void add(RuntimeInfo runtimeInfo) {
    Lock lock = runtimeInfosLock.writeLock();
    lock.lock();
    try {
      runtimeInfos.put(runtimeInfo.getType(), runtimeInfo.getController().getRunId(), runtimeInfo);
    } finally {
      lock.unlock();
    }
  }

  private void remove(RuntimeInfo info, Runnable cleanUpTask) {
    Lock lock = runtimeInfosLock.writeLock();
    lock.lock();
    try {
      LOG.debug("Removing RuntimeInfo: {} {} {}",
                info.getType(), info.getProgramId().getProgram(), info.getController().getRunId());
      RuntimeInfo removed = runtimeInfos.remove(info.getType(), info.getController().getRunId());
      LOG.debug("RuntimeInfo removed: {}", removed);
    } finally {
      lock.unlock();
      cleanUpTask.run();
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
   * @param controller the {@link TwillController} controlling the corresponding twill application
   * @return a {@link RuntimeInfo} or {@code null} if not able to create the {@link RuntimeInfo} due to unexpected
   *         and unrecoverable error/bug.
   */
  @Nullable
  protected RuntimeInfo createRuntimeInfo(ProgramId programId, RunId runId, TwillController controller) {
    try {
      ProgramController programController = createController(programId, runId, controller);
      SimpleRuntimeInfo runtimeInfo = programController == null ? null : new SimpleRuntimeInfo(programController,
                                                                                               programId,
                                                                                               controller.getRunId());
      if (runtimeInfo != null) {
        updateRuntimeInfo(programId.getType(), runId, runtimeInfo);
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
    ProgramRunner programRunner;
    try {
      programRunner = programRunnerFactory.create(programId.getType());
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

    return ((ProgramControllerCreator) programRunner).createProgramController(controller, programId, runId);
  }
}
