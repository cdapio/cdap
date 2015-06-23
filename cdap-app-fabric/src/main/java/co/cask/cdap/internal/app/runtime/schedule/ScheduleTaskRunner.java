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
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.services.PropertiesResolver;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nullable;

/**
 * Task runner that runs a schedule.
 */
public final class ScheduleTaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ScheduleTaskRunner.class);

  private final ProgramLifecycleService lifecycleService;
  private final Store store;
  private final ListeningExecutorService executorService;
  private final PropertiesResolver propertiesResolver;

  public ScheduleTaskRunner(Store store, ProgramLifecycleService lifecycleService,
                            PropertiesResolver propertiesResolver, ListeningExecutorService taskExecutor) {
    this.store = store;
    this.lifecycleService = lifecycleService;
    this.propertiesResolver = propertiesResolver;
    this.executorService = taskExecutor;
  }

  /**
   * Executes the giving program without blocking until its completion.
   *
   * @param programId Program Id
   * @param programType Program type.
   * @param systemOverrides Arguments that would be supplied as system runtime arguments for the program.
   * @return a {@link ListenableFuture} object that completes when the program completes
   * @throws TaskExecutionException if program is already running or program is not found.
   * @throws IOException if program failed to start.
   */
  public ListenableFuture<?> run(Id.Program programId, ProgramType programType, Map<String, String> systemOverrides)
    throws TaskExecutionException, IOException {
    Map<String, String> userArgs = Maps.newHashMap();
    Map<String, String> systemArgs = Maps.newHashMap();

    String scheduleName = systemOverrides.get(ProgramOptionConstants.SCHEDULE_NAME);
    ApplicationSpecification appSpec = store.getApplication(programId.getApplication());
    if (appSpec == null || appSpec.getSchedules().get(scheduleName) == null) {
      throw new TaskExecutionException(UserMessages.getMessage(UserErrors.PROGRAM_NOT_FOUND), false);
    }

    ScheduleSpecification spec = appSpec.getSchedules().get(scheduleName);

    // Schedule properties are overriden by resolved preferences
    userArgs.putAll(spec.getProperties());
    userArgs.putAll(propertiesResolver.getUserProperties(programId));

    systemArgs.putAll(propertiesResolver.getSystemProperties(programId));
    systemArgs.putAll(systemOverrides);

    return execute(programId, programType, systemArgs, userArgs);
  }

  /**
   * Executes a program without blocking until its completion.
   *
   * @return a {@link ListenableFuture} object that completes when the program completes
   */
  private ListenableFuture<?> execute(final Id.Program id, final ProgramType type, Map<String, String> sysArgs,
                                      Map<String, String> userArgs) throws IOException, TaskExecutionException {
    ProgramRuntimeService.RuntimeInfo runtimeInfo;
    try {
      runtimeInfo = lifecycleService.start(id, sysArgs, userArgs, false);
    } catch (ProgramNotFoundException e) {
      throw new TaskExecutionException(UserMessages.getMessage(UserErrors.PROGRAM_NOT_FOUND), e, false);
    }

    final ProgramController controller = runtimeInfo.getController();
    final CountDownLatch latch = new CountDownLatch(1);

    controller.addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State state, @Nullable Throwable cause) {
        if (state == ProgramController.State.COMPLETED) {
          completed();
        }
        if (state == ProgramController.State.ERROR) {
          error(controller.getFailureCause());
        }
      }

      @Override
      public void completed() {
        latch.countDown();
      }

      @Override
      public void error(Throwable cause) {
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
