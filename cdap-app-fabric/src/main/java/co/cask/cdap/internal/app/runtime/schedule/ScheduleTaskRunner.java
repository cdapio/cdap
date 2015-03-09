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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Task runner that runs a schedule.
 */
public final class ScheduleTaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ScheduleTaskRunner.class);

  private final ProgramRuntimeService runtimeService;
  private final Store store;
  private final PreferencesStore preferencesStore;
  private final ListeningExecutorService executorService;

  public ScheduleTaskRunner(Store store, ProgramRuntimeService runtimeService, PreferencesStore preferencesStore,
                            ListeningExecutorService taskExecutor) {
    this.runtimeService = runtimeService;
    this.store = store;
    this.preferencesStore = preferencesStore;
    this.executorService = taskExecutor;
  }

  /**
   * Executes the giving program without blocking until its completion.
   *
   * @param programId Program Id
   * @param programType Program type.
   * @param arguments Arguments that would be supplied as system runtime arguments for the program.
   * @return a {@link ListenableFuture} object that completes when the program completes
   * @throws TaskExecutionException If fails to execute the program.
   */
  public ListenableFuture<?> run(Id.Program programId, ProgramType programType, Arguments arguments)
    throws TaskExecutionException {
    Map<String, String> userArgs = Maps.newHashMap();
    Program program;
    try {
      program = store.loadProgram(programId, ProgramType.WORKFLOW);
      Preconditions.checkNotNull(program, "Program not found");

      String scheduleName = arguments.getOption(ProgramOptionConstants.SCHEDULE_NAME);
      ScheduleSpecification spec = store.getApplication(programId.getApplication()).getSchedules().get(scheduleName);
      Preconditions.checkNotNull(spec, "Schedule not found");

      userArgs.putAll(spec.getProperties());

      Map<String, String> runtimeArgs = preferencesStore.getResolvedProperties(programId.getNamespaceId(),
                                                        programId.getApplicationId(), programType.getCategoryName(),
                                                        programId.getId());

      userArgs.putAll(runtimeArgs);

      boolean runMultipleProgramInstances =
        Boolean.parseBoolean(userArgs.get(ProgramOptionConstants.CONCURRENT_RUNS_ENABLED));

      if (!runMultipleProgramInstances) {
        ProgramRuntimeService.RuntimeInfo existingRuntimeInfo = findRuntimeInfo(programId, programType);
        if (existingRuntimeInfo != null) {
          throw new TaskExecutionException(UserMessages.getMessage(UserErrors.ALREADY_RUNNING), false);
        }
      }
    } catch (Throwable t) {
      Throwables.propagateIfInstanceOf(t, TaskExecutionException.class);
      throw new TaskExecutionException(UserMessages.getMessage(UserErrors.PROGRAM_NOT_FOUND), t, false);
    }

    return execute(program, new SimpleProgramOptions(programId.getId(), arguments, new BasicArguments(userArgs)));
  }

  /**
   * Returns runtime information for the given program if it is running,
   * or {@code null} if no instance of it is running.
   */
  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(Id.Program programId, ProgramType programType) {
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(programType).values();

    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (programId.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
  }

  /**
   * Executes a program without blocking until its completion.
   * 
   * @return a {@link ListenableFuture} object that completes when the program completes
   */
  private ListenableFuture<?> execute(final Program program, ProgramOptions options)
    throws TaskExecutionException {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.run(program, options);

    final ProgramController controller = runtimeInfo.getController();
    final Id.Program programId = program.getId();
    final String runId = controller.getRunId().getId();
    final CountDownLatch latch = new CountDownLatch(1);

    controller.addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State state, @Nullable Throwable cause) {
        store.setStart(programId, runId, TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS));
        if (state == ProgramController.State.COMPLETED) {
          completed();
        }
        if (state == ProgramController.State.ERROR) {
          error(controller.getFailureCause());
        }
      }

      @Override
      public void completed() {
        store.setStop(programId, runId,
                      TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                      ProgramController.State.COMPLETED.getRunStatus());
        LOG.debug("Program {} {} {} completed successfully.",
                  programId.getNamespaceId(), programId.getApplicationId(), programId.getId());
        latch.countDown();
      }

      @Override
      public void error(Throwable cause) {
        store.setStop(programId, runId,
                      TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                      ProgramController.State.ERROR.getRunStatus());
        LOG.debug("Program {} {} {} execution failed.",
                  programId.getNamespaceId(), programId.getApplicationId(), programId.getId(),
                  cause);

        latch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    return executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        latch.await();
        return null;
      }
    });
  }
}
