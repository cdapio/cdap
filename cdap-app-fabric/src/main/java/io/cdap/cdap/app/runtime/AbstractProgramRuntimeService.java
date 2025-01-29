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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.app.deploy.ProgramRunDispatcherContext;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.twill.TwillAppNames;
import io.cdap.cdap.internal.app.deploy.ProgramRunDispatcherFactory;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.service.SimpleRuntimeInfo;
import io.cdap.cdap.proto.InMemoryProgramLiveInfo;
import io.cdap.cdap.proto.NotRunningProgramLiveInfo;
import io.cdap.cdap.proto.ProgramLiveInfo;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ProgramRuntimeService that keeps an in memory map for all running programs.
 */
public abstract class AbstractProgramRuntimeService extends AbstractIdleService implements
    ProgramRuntimeService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramRuntimeService.class);
  private static final EnumSet<ProgramController.State> COMPLETED_STATES = EnumSet.of(
      ProgramController.State.COMPLETED,
      ProgramController.State.KILLED,
      ProgramController.State.ERROR);
  private final CConfiguration cConf;
  private final ReadWriteLock runtimeInfosLock;
  private final Table<ProgramType, RunId, RuntimeInfo> runtimeInfos;
  private final ProgramRunnerFactory programRunnerFactory;
  private final ProgramStateWriter programStateWriter;
  private final ProgramRunDispatcherFactory programRunDispatcherFactory;
  private ProgramRunnerFactory remoteProgramRunnerFactory;
  private TwillRunnerService remoteTwillRunnerService;
  private ExecutorService executor;

  protected AbstractProgramRuntimeService(CConfiguration cConf,
      ProgramRunnerFactory programRunnerFactory,
      ProgramStateWriter programStateWriter,
      ProgramRunDispatcherFactory programRunDispatcherFactory) {
    this.cConf = cConf;
    this.programRunDispatcherFactory = programRunDispatcherFactory;
    this.runtimeInfosLock = new ReentrantReadWriteLock();
    this.runtimeInfos = HashBasedTable.create();
    this.programRunnerFactory = programRunnerFactory;
    this.programStateWriter = programStateWriter;
  }

  protected abstract boolean isDistributed();

  /**
   * Optional guice injection for the {@link ProgramRunnerFactory} used for remote execution. It is
   * optional because in unit-test we don't have need for that.
   */
  @Inject(optional = true)
  void setRemoteProgramRunnerFactory(
      @Constants.AppFabric.RemoteExecution ProgramRunnerFactory runnerFactory) {
    this.remoteProgramRunnerFactory = runnerFactory;
  }

  /**
   * Optional guice injection for the {@link TwillRunnerService} used for remote execution. It is
   * optional because in unit-test we don't have need for that.
   */
  @Inject(optional = true)
  void setRemoteTwillRunnerService(
      @Constants.AppFabric.RemoteExecution TwillRunnerService twillRunnerService) {
    this.remoteTwillRunnerService = twillRunnerService;
  }

  @Override
  public final RuntimeInfo run(ProgramDescriptor programDescriptor, ProgramOptions options,
      RunId runId) {
    ProgramId programId = programDescriptor.getProgramId();
    ProgramRunId programRunId = programId.run(runId);
    Lock lock = runtimeInfosLock.writeLock();
    lock.lock();
    try {
      RuntimeInfo runtimeInfo = lookup(programRunId.getParent(), runId);
      if (runtimeInfo != null) {
        return runtimeInfo;
      }

      ProgramRunDispatcherContext dispatcherContext = new ProgramRunDispatcherContext(
          programDescriptor, options, runId,
          isDistributed());
      DelayedProgramController controller = new DelayedProgramController(programRunId);
      runtimeInfo = createRuntimeInfo(controller, programId,
          dispatcherContext::executeCleanupTasks);
      updateRuntimeInfo(runtimeInfo);
      executor.execute(() -> {
        try {
          controller.setProgramController(
              programRunDispatcherFactory.getProgramRunDispatcher(programId.getType())
                  .dispatchProgram(dispatcherContext));
        } catch (Exception e) {
          controller.failed(e);
          programStateWriter.error(programRunId, e);
          LOG.error("Exception while trying to run program run {}", programRunId, e);
        }
      });
      return runtimeInfo;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public ProgramLiveInfo getLiveInfo(ProgramId programId) {
    return isRunning(programId) ? new InMemoryProgramLiveInfo(programId)
        : new NotRunningProgramLiveInfo(programId);
  }

  protected RuntimeInfo createRuntimeInfo(ProgramController controller, ProgramId programId,
      Runnable cleanUpTask) {
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
        if (result.computeIfAbsent(runId, rid -> createRuntimeInfo(programId, runId, controller))
            == null) {
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
  public void resetProgramLogLevels(ProgramId programId, Set<String> loggerNames,
      @Nullable String runId) throws Exception {
    if (!EnumSet.of(ProgramType.SERVICE, ProgramType.WORKER).contains(programId.getType())) {
      throw new BadRequestException(
          String.format("Resetting log levels for program type %s is not supported",
              programId.getType().getPrettyName()));
    }
    resetLogLevels(programId, loggerNames, runId);
  }

  @Override
  public void updateProgramLogLevels(ProgramId programId, Map<String, LogEntry.Level> logLevels,
      @Nullable String runId) throws Exception {
    if (!EnumSet.of(ProgramType.SERVICE, ProgramType.WORKER).contains(programId.getType())) {
      throw new BadRequestException(
          String.format("Updating log levels for program type %s is not supported",
              programId.getType().getPrettyName()));
    }
    updateLogLevels(programId, logLevels, runId);
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
   * @return the corresponding {@link RuntimeInfo} or {@code null} if no runtime information was
   *     found
   */
  @Nullable
  protected RuntimeInfo lookupFromTwillRunner(TwillRunner twillRunner, ProgramRunId programRunId,
      RunId twillRunId) {
    TwillController twillController = twillRunner.lookup(
        TwillAppNames.toTwillAppName(programRunId.getParent()),
        twillRunId);
    if (twillController == null) {
      return null;
    }

    return createRuntimeInfo(programRunId.getParent(), RunIds.fromString(programRunId.getRun()),
        twillController);
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
   * @param twillController the {@link TwillController} controlling the corresponding twill
   *     application
   * @return a {@link RuntimeInfo} or {@code null} if not able to create the {@link RuntimeInfo} due
   *     to unexpected and unrecoverable error/bug.
   */
  @Nullable
  protected RuntimeInfo createRuntimeInfo(ProgramId programId, RunId runId,
      TwillController twillController) {
    try {
      ProgramController controller = createController(programId, runId, twillController);
      RuntimeInfo runtimeInfo =
          controller == null ? null : createRuntimeInfo(controller, programId, () -> {
          });

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
   * @param controller the {@link TwillController} controlling the corresponding twill
   *     application
   * @return a {@link ProgramController} or {@code null} if there is unexpected error/bug
   */
  @Nullable
  private ProgramController createController(ProgramId programId, RunId runId,
      TwillController controller) {
    ProgramRunnerFactory factory = runId.equals(controller.getRunId())
        ? remoteProgramRunnerFactory : programRunnerFactory;

    ProgramRunner programRunner;
    try {
      programRunner = factory.create(programId.getType());
    } catch (IllegalArgumentException e) {
      // This shouldn't happen. If it happen, it means CDAP was incorrectly install such that some of the program
      // type is not support (maybe due to version mismatch in upgrade).
      LOG.error("Unsupported program type {} for program {}. "
              + "It is likely caused by incorrect CDAP installation or upgrade to incompatible CDAP version",
          programId.getType(), programId);
      return null;
    }

    if (!(programRunner instanceof ProgramControllerCreator)) {
      // This is also unexpected. If it happen, it means the CDAP core or the runtime provider extension was wrongly
      // implemented
      ResourceReport resourceReport = controller.getResourceReport();
      LOG.error(
          "Unable to create ProgramController for program {} for twill application {}. It is likely caused by "

              + "invalid CDAP program runtime extension.",
          programId, resourceReport == null ? "'unknown twill application'"
              : resourceReport.getApplicationId());
      return null;
    }

    return ((ProgramControllerCreator) programRunner).createProgramController(programId.run(runId),
        controller);
  }

  /**
   * Cleanup the given {@link RuntimeInfo} if it is {@link Closeable}.
   */
  private void cleanupRuntimeInfo(@Nullable RuntimeInfo info) {
    if (info instanceof Closeable) {
      Closeables.closeQuietly((Closeable) info);
    }
  }

  /**
   * Helper method to get the {@link LogLevelUpdater} for the program.
   */
  private LogLevelUpdater getLogLevelUpdater(RuntimeInfo runtimeInfo) throws Exception {
    ProgramController programController = runtimeInfo.getController();
    if (!(programController instanceof LogLevelUpdater)) {
      throw new BadRequestException(
          "Update log levels at runtime is only supported in distributed mode");
    }
    return ((LogLevelUpdater) programController);
  }

  /**
   * Helper method to update log levels for Worker or Service.
   */
  private void updateLogLevels(ProgramId programId, Map<String, LogEntry.Level> logLevels,
      @Nullable String runId) throws Exception {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId, runId).values()
        .stream()
        .findFirst().orElse(null);
    if (runtimeInfo != null) {
      LogLevelUpdater logLevelUpdater = getLogLevelUpdater(runtimeInfo);
      logLevelUpdater.updateLogLevels(logLevels, null);
    }
  }

  /**
   * Helper method to reset log levels for Worker or Service.
   */
  private void resetLogLevels(ProgramId programId, Set<String> loggerNames, @Nullable String runId)
      throws Exception {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId, runId).values()
        .stream()
        .findFirst().orElse(null);
    if (runtimeInfo != null) {
      LogLevelUpdater logLevelUpdater = getLogLevelUpdater(runtimeInfo);
      logLevelUpdater.resetLogLevels(loggerNames, null);
    }
  }

  @Override
  public void setInstances(ProgramId programId, int instances, int oldInstances)
      throws ExecutionException, InterruptedException, BadRequestException {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId);
    if (runtimeInfo != null) {
      runtimeInfo.getController().command(ProgramOptionConstants.INSTANCES,
          ImmutableMap.of("runnable", programId.getProgram(),
              "newInstances", String.valueOf(instances),
              "oldInstances", String.valueOf(oldInstances))).get();
    }
  }

  private Map<RunId, ProgramRuntimeService.RuntimeInfo> findRuntimeInfo(
      ProgramId programId, @Nullable String runId) throws BadRequestException {

    if (runId != null) {
      RunId run;
      try {
        run = RunIds.fromString(runId);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Error parsing run-id.", e);
      }
      ProgramRuntimeService.RuntimeInfo runtimeInfo = lookup(programId, run);
      return runtimeInfo == null ? Collections.emptyMap()
          : Collections.singletonMap(run, runtimeInfo);
    }
    return new HashMap<>(list(programId));
  }

  @Nullable
  protected ProgramRuntimeService.RuntimeInfo findRuntimeInfo(ProgramId programId)
      throws BadRequestException {
    return findRuntimeInfo(programId, null).values().stream().findFirst().orElse(null);
  }
}
