/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package co.cask.cdap.app.runtime;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.security.Impersonator;
import co.cask.cdap.internal.app.deploy.pipeline.NamespacedImpersonator;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDetail;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactMeta;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRangeCodec;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.Artifacts;
import co.cask.cdap.internal.app.runtime.service.SimpleRuntimeInfo;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.api.RunId;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;

/**
 * A ProgramRuntimeService that keeps an in memory map for all running programs.
 */
public abstract class AbstractProgramRuntimeService extends AbstractIdleService implements ProgramRuntimeService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramRuntimeService.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(ArtifactRange.class, new ArtifactRangeCodec())
    .create();

  private static final EnumSet<ProgramController.State> COMPLETED_STATES = EnumSet.of(ProgramController.State.COMPLETED,
                                                                                      ProgramController.State.KILLED,
                                                                                      ProgramController.State.ERROR);
  private final CConfiguration cConf;
  private final ReadWriteLock runtimeInfosLock;
  private final Table<ProgramType, RunId, RuntimeInfo> runtimeInfos;
  private final ProgramRunnerFactory programRunnerFactory;
  private final ArtifactRepository artifactRepository;
  private final Impersonator impersonator;

  protected AbstractProgramRuntimeService(CConfiguration cConf, ProgramRunnerFactory programRunnerFactory,
                                          ArtifactRepository artifactRepository, Impersonator impersonator) {
    this.cConf = cConf;
    this.runtimeInfosLock = new ReentrantReadWriteLock();
    this.runtimeInfos = HashBasedTable.create();
    this.programRunnerFactory = programRunnerFactory;
    this.artifactRepository = artifactRepository;
    this.impersonator = impersonator;
  }

  @Override
  public final RuntimeInfo run(ProgramDescriptor programDescriptor, ProgramOptions options) {
    ProgramId programId = programDescriptor.getProgramId();

    ProgramRunner runner = programRunnerFactory.create(programId.getType());
    Preconditions.checkNotNull(runner, "Fail to get ProgramRunner for type " + programId.getType());

    RunId runId = RunIds.generate();
    File tempDir = createTempDirectory(programId, runId);
    Runnable cleanUpTask = createCleanupTask(tempDir, runner);
    try {
      // Get the artifact details and save it into the program options.
      ArtifactId artifactId = programDescriptor.getArtifactId();
      ArtifactDetail artifactDetail = getArtifactDetail(artifactId);
      ProgramOptions runtimeProgramOptions = updateProgramOptions(options, runId, artifactId, artifactDetail.getMeta());

      // Take a snapshot of all the plugin artifacts used by the program
      ProgramOptions optionsWithPlugins = createPluginSnapshot(runtimeProgramOptions, programId, tempDir,
                                                               programDescriptor.getApplicationSpecification());

      // Create and run the program
      Program executableProgram = createProgram(cConf, runner, programDescriptor,
                                                artifactDetail.getDescriptor().getLocation(), tempDir);
      cleanUpTask = createCleanupTask(cleanUpTask, executableProgram);
      RuntimeInfo runtimeInfo = createRuntimeInfo(runner.run(executableProgram, optionsWithPlugins), programId);
      monitorProgram(runtimeInfo, cleanUpTask);
      return runtimeInfo;
    } catch (Exception e) {
      cleanUpTask.run();
      LOG.error("Exception while trying to run program", e);
      throw Throwables.propagate(e);
    }
  }

  protected ArtifactDetail getArtifactDetail(ArtifactId artifactId) throws Exception {
    return artifactRepository.getArtifact(artifactId.toId());
  }

  /**
   * Creates a {@link Program} for the given {@link ProgramRunner} from the given program jar {@link Location}.
   */
  protected Program createProgram(final CConfiguration cConf, final ProgramRunner programRunner,
                                  final ProgramDescriptor programDescriptor,
                                  final Location programJarLocation, final File tempDir) throws IOException {
    // Take a snapshot of the JAR file to avoid program mutation
    final File unpackedDir = new File(tempDir, "unpacked");
    unpackedDir.mkdirs();
    try {
      File programJar = Locations.linkOrCopy(programJarLocation, new File(tempDir, "program.jar"));
      // Unpack the JAR file
      BundleJarUtil.unJar(Files.newInputStreamSupplier(programJar), unpackedDir);
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      // should not happen
      throw Throwables.propagate(e);
    }
    return Programs.create(cConf, programRunner, programDescriptor, programJarLocation, unpackedDir);
  }

  private Runnable createCleanupTask(final Object... resources) {
    return new Runnable() {
      @Override
      public void run() {
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
      final File destFile = new File(tempDir, Artifacts.getFileName(plugin.getArtifactId()));
      // Skip if the file has already been copied.
      if (!files.add(destFile.getName())) {
        continue;
      }

      try {
        ArtifactId artifactId = Artifacts.toArtifactId(programId.getNamespaceId(), plugin.getArtifactId());
        final ArtifactDetail detail = artifactRepository.getArtifact(artifactId.toId());
        try {
          new NamespacedImpersonator(artifactId.getParent(), impersonator).impersonate(
            new Callable<Void>() {
              @Override
              public Void call() throws IOException {
                Locations.linkOrCopy(detail.getDescriptor().getLocation(), destFile);
                return null;
              }});
        } catch (IOException ioe) {
          throw ioe;
        } catch (Exception e) {
          // should not happen
          throw Throwables.propagate(e);
        }
      } catch (ArtifactNotFoundException e) {
        throw new IllegalArgumentException(String.format("Artifact %s could not be found", plugin.getArtifactId()), e);
      }
    }
    LOG.debug("Plugin artifacts of {} copied to {}", programId, tempDir.getAbsolutePath());
    builder.put(ProgramOptionConstants.PLUGIN_DIR, tempDir.getAbsolutePath());
    return new SimpleProgramOptions(options.getName(), new BasicArguments(builder.build()),
                                    options.getUserArguments(), options.isDebug());
  }

  protected Map<String, String> getExtraProgramOptions() {
    return Collections.emptyMap();
  }

  /**
   * Updates the given {@link ProgramOptions} and return a new instance.
   * It copies the {@link ProgramOptions} and add all options returned by {@link #getExtraProgramOptions()}.
   * It then adds the {@link RunId} to it.
   *
   * @param options The {@link ProgramOptions} in which the RunId to be included
   * @param runId   The RunId to be included
   * @param artifactId The {@link ArtifactId} of the artifact that contains the program to be executed
   * @param artifactMeta The {@link ArtifactMeta} of the artifact that contains the program to be executed
   * @return the copy of the program options with RunId included in them
   */
  private ProgramOptions updateProgramOptions(ProgramOptions options, RunId runId,
                                              ArtifactId artifactId, ArtifactMeta artifactMeta) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(options.getArguments().asMap());
    builder.putAll(getExtraProgramOptions());
    builder.put(ProgramOptionConstants.RUN_ID, runId.getId());
    builder.put(ProgramOptionConstants.ARTIFACT_ID, GSON.toJson(artifactId));
    builder.put(ProgramOptionConstants.ARTIFACT_META, GSON.toJson(artifactMeta));

    return new SimpleProgramOptions(options.getName(), new BasicArguments(builder.build()), options.getUserArguments(),
                                    options.isDebug());
  }

  protected RuntimeInfo createRuntimeInfo(ProgramController controller, ProgramId programId) {
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

  @Override
  public RuntimeInfo lookup(Id.Program programId, RunId runId) {
    Lock lock = runtimeInfosLock.readLock();
    lock.lock();
    try {
      return runtimeInfos.get(programId.getType(), runId);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Map<RunId, RuntimeInfo> list(ProgramType type) {
    Lock lock = runtimeInfosLock.readLock();
    lock.lock();
    try {
      return ImmutableMap.copyOf(runtimeInfos.row(type));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Map<RunId, RuntimeInfo> list(final Id.Program program) {
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
      for (Map.Entry<RunId, ProgramRuntimeService.RuntimeInfo> entry : list(type).entrySet()) {
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
    // No-op
  }

  @Override
  protected void shutDown() throws Exception {
    // No-op
  }

  protected void updateRuntimeInfo(ProgramType type, RunId runId, RuntimeInfo runtimeInfo) {
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
                info.getType(), info.getProgramId().getId(), info.getController().getRunId());
      RuntimeInfo removed = runtimeInfos.remove(info.getType(), info.getController().getRunId());
      LOG.debug("RuntimeInfo removed: {}", removed);
    } finally {
      lock.unlock();
      cleanUpTask.run();
    }
  }

  protected boolean isRunning(Id.Program programId) {
    for (Map.Entry<RunId, RuntimeInfo> entry : list(programId.getType()).entrySet()) {
      if (entry.getValue().getProgramId().equals(programId)) {
        return true;
      }
    }
    return false;
  }
}
