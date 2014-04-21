package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.store.Store;
import com.continuuity.internal.UserErrors;
import com.continuuity.internal.UserMessages;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
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

  public ScheduleTaskRunner(Store store, ProgramRuntimeService runtimeService) {
    this.runtimeService = runtimeService;
    this.store = store;
  }

  /**
   * Executes the giving program and block until its completion.
   *
   * @param programId Program Id
   * @param programType Program type.
   * @param arguments Arguments that would be supplied as system runtime arguments for the program.
   * @throws JobExecutionException If fails to execute the program.
   */
  public void run(Id.Program programId, Type programType, Arguments arguments) throws JobExecutionException {
    ProgramRuntimeService.RuntimeInfo existingRuntimeInfo = findRuntimeInfo(programId, programType);
    if (existingRuntimeInfo != null) {
      throw new JobExecutionException(UserMessages.getMessage(UserErrors.ALREADY_RUNNING), false);
    }
    Map<String, String> userArgs;
    Program program;
    try {
      program =  store.loadProgram(programId, Type.WORKFLOW);
      Preconditions.checkNotNull(program, "Program not found");

      userArgs = store.getRunArguments(programId);

    } catch (Throwable t) {
      throw new JobExecutionException(UserMessages.getMessage(UserErrors.PROGRAM_NOT_FOUND), t, false);
    }

    executeAndBlock(program, new SimpleProgramOptions(programId.getId(), arguments, new BasicArguments(userArgs)));
  }

  /**
   * Returns runtime information for the given program if it is running,
   * or {@code null} if no instance of it is running.
   */
  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(Id.Program programId, Type programType) {
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
                      ProgramController.State.STOPPED.toString());
        LOG.debug("Program {} {} {} completed successfully.",
                  programId.getAccountId(), programId.getApplicationId(), programId.getId());
        latch.countDown();
      }

      @Override
      public void error(Throwable cause) {
        store.setStop(programId, runId,
                      TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                      ProgramController.State.STOPPED.toString());
        LOG.debug("Program {} {} {} execution failed.",
                  programId.getAccountId(), programId.getApplicationId(), programId.getId(),
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
