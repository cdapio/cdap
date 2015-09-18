/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDetail;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.Artifacts;
import co.cask.cdap.internal.app.runtime.service.SimpleRuntimeInfo;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;

/**
 * A ProgramRuntimeService that keeps an in memory map for all running programs.
 */
public abstract class AbstractProgramRuntimeService extends AbstractIdleService implements ProgramRuntimeService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramRuntimeService.class);
  private static final EnumSet<ProgramController.State> COMPLETED_STATES = EnumSet.of(ProgramController.State.COMPLETED,
                                                                                      ProgramController.State.KILLED,
                                                                                      ProgramController.State.ERROR);
  private final CConfiguration cConf;
  private final ReadWriteLock runtimeInfosLock;
  private final Table<ProgramType, RunId, RuntimeInfo> runtimeInfos;
  private final ProgramRunnerFactory programRunnerFactory;
  private final ArtifactRepository artifactRepository;

  protected AbstractProgramRuntimeService(CConfiguration cConf, ProgramRunnerFactory programRunnerFactory,
                                          ArtifactRepository artifactRepository) {
    this.cConf = cConf;
    this.runtimeInfosLock = new ReentrantReadWriteLock();
    this.runtimeInfos = HashBasedTable.create();
    this.programRunnerFactory = programRunnerFactory;
    this.artifactRepository = artifactRepository;
  }

  @Override
  public RuntimeInfo run(Program program, ProgramOptions options) {
    ProgramRunner runner = programRunnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));
    Preconditions.checkNotNull(runner, "Fail to get ProgramRunner for type " + program.getType());
    RunId runId = RunIds.generate();
    ProgramOptions optionsWithRunId = addRunId(options, runId);
    File tempDir = createTempDirectory(program.getId(), runId);
    Runnable cleanUpTask = createCleanupTask(tempDir);
    ProgramOptions optionsWithPlugins = null;
    try {
      optionsWithPlugins = createPluginSnapshot(optionsWithRunId, program.getId(), tempDir,
                                                program.getApplicationSpecification());
    } catch (IOException e) {
      cleanUpTask.run();
      LOG.error("Exception while trying to createPluginSnapshot", e);
      Throwables.propagate(e);
    }

    final RuntimeInfo runtimeInfo = createRuntimeInfo(runner.run(program, optionsWithPlugins), program);
    monitorProgram(runtimeInfo, cleanUpTask);
    return runtimeInfo;
  }

  private Runnable createCleanupTask(final File... resources) {
    return new Runnable() {
      @Override
      public void run() {
        for (File file : resources) {
          if (file == null) {
            continue;
          }

          try {
            if (file.isDirectory()) {
              DirUtils.deleteDirectoryContents(file);
            } else {
              file.delete();
            }
          } catch (Throwable t) {
            LOG.warn("Exception when cleaning up resource {}", file, t);
          }
        }
      }
    };
  }

  /**
   * Creates a local temporary directory for this program run.
   */
  private File createTempDirectory(Id.Program programId, RunId runId) {
    File tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                            cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    File dir = new File(tempDir, String.format("%s.%s.%s.%s.%s",
                                               programId.getType().name().toLowerCase(),
                                               programId.getNamespaceId(), programId.getApplicationId(),
                                               programId.getId(), runId.getId()));
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
  private ProgramOptions createPluginSnapshot(ProgramOptions options, Id.Program programId, File tempDir,
                                              @Nullable ApplicationSpecification appSpec) throws IOException {
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
        ArtifactDetail detail = artifactRepository.getArtifact(Id.Artifact.from(programId.getNamespace(),
                                                                                plugin.getArtifactId()));
        Files.copy(Locations.newInputSupplier(detail.getDescriptor().getLocation()), destFile);
      } catch (ArtifactNotFoundException e) {
        throw new IllegalArgumentException(String.format("Artifact %s could not be found", plugin.getArtifactId()), e);
      }
    }
    LOG.debug("Plugin artifacts of {} copied to {}", programId, tempDir.getAbsolutePath());
    builder.put(ProgramOptionConstants.PLUGIN_DIR, tempDir.getAbsolutePath());
    return new SimpleProgramOptions(options.getName(), new BasicArguments(builder.build()),
                                    options.getUserArguments(), options.isDebug());
  }

  /**
   * Return the copy of the {@link ProgramOptions} including RunId in it.
   * @param options The {@link ProgramOptions} in which the RunId to be included
   * @param runId   The RunId to be included
   * @return the copy of the program options with RunId included in them
   */
  private ProgramOptions addRunId(ProgramOptions options, RunId runId) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(options.getArguments().asMap());
    builder.put(ProgramOptionConstants.RUN_ID, runId.getId());

    return new SimpleProgramOptions(options.getName(), new BasicArguments(builder.build()), options.getUserArguments(),
                                    options.isDebug());
  }

  protected RuntimeInfo createRuntimeInfo(ProgramController controller, Program program) {
    return new SimpleRuntimeInfo(controller, program);
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
  public boolean checkAnyRunning(Predicate<Id.Program> predicate, ProgramType... types) {
    for (ProgramType type : types) {
      for (Map.Entry<RunId, ProgramRuntimeService.RuntimeInfo> entry :  list(type).entrySet()) {
        ProgramController.State programState = entry.getValue().getController().getState();
        if (programState.isDone()) {
          continue;
        }
        Id.Program programId = entry.getValue().getProgramId();
        if (predicate.apply(programId)) {
          LOG.trace("Program still running in checkAnyRunning: {} {} {} {}",
                    programId.getApplicationId(), type, programId.getId(), entry.getValue().getController().getRunId());
          return true;
        }
      }
    }
    return false;
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
