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
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import org.apache.twill.common.Threads;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Task runner that runs a schedule.
 */
public final class ScheduleTaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ScheduleTaskRunner.class);

  private final ProgramRuntimeService runtimeService;
  private final Store store;
  private final PreferencesStore preferencesStore;

  public ScheduleTaskRunner(Store store, ProgramRuntimeService runtimeService, PreferencesStore preferencesStore) {
    this.runtimeService = runtimeService;
    this.store = store;
    this.preferencesStore = preferencesStore;
  }

  /**
   * Executes the giving program and block until its completion.
   *
   * @param programId Program Id
   * @param programType Program type.
   * @param arguments Arguments that would be supplied as system runtime arguments for the program.
   * @throws JobExecutionException If fails to execute the program.
   */
  public void run(Id.Program programId, ProgramType programType, Arguments arguments) throws JobExecutionException {
    ProgramRuntimeService.RuntimeInfo existingRuntimeInfo = findRuntimeInfo(programId, programType);
    if (existingRuntimeInfo != null) {
      throw new JobExecutionException(UserMessages.getMessage(UserErrors.ALREADY_RUNNING), false);
    }
    Map<String, String> userArgs;
    Program program;
    try {
      program =  store.loadProgram(programId, ProgramType.WORKFLOW);
      Preconditions.checkNotNull(program, "Program not found");

      userArgs = preferencesStore.getResolvedProperties(programId.getAccountId(), programId.getApplicationId(),
                                                          programType.getCategoryName(), programId.getId());

    } catch (Throwable t) {
      throw new JobExecutionException(UserMessages.getMessage(UserErrors.PROGRAM_NOT_FOUND), t, false);
    }

    executeAndBlock(program, new SimpleProgramOptions(programId.getId(), arguments, new BasicArguments(userArgs)));
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
   * Executes a program and block until it is completed.
   */
  private void executeAndBlock(final Program program, ProgramOptions options) throws JobExecutionException {
    ProgramController controller = runtimeService.run(program, options).getController();
    store.setStart(program.getId(), controller.getRunId().getId(),
                   TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS));

    final Id.Program programId = program.getId();
    final String runId = controller.getRunId().getId();
    final CountDownLatch latch = new CountDownLatch(1);
    controller.addListener(new AbstractListener() {
      @Override
      public void stopped() {
        store.setStop(programId, runId,
                      TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                      ProgramController.State.STOPPED);
        LOG.debug("Program {} {} {} completed successfully.",
                  programId.getNamespaceId(), programId.getApplicationId(), programId.getId());
        latch.countDown();
      }

      @Override
      public void error(Throwable cause) {
        store.setStop(programId, runId,
                      TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                      ProgramController.State.STOPPED);
        LOG.debug("Program {} {} {} execution failed.",
                  programId.getNamespaceId(), programId.getApplicationId(), programId.getId(),
                  cause);

        latch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new JobExecutionException(e, false);
    }
  }
}
