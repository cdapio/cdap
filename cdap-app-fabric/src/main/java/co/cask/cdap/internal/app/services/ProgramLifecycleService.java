/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.runtime.RunIds;
import co.cask.cdap.app.runtime.scheduler.SchedulerQueueResolver;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Service that manages lifecycle of Programs.
 */
public class ProgramLifecycleService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramLifecycleService.class);

  private final Store store;
  private final ProgramRuntimeService runtimeService;
  private final SchedulerQueueResolver queueResolver;

  @Inject
  public ProgramLifecycleService(Store store, CConfiguration cConf, ProgramRuntimeService runtimeService) {
    this.store = store;
    this.runtimeService = runtimeService;
    this.queueResolver = new SchedulerQueueResolver(cConf, store);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting ProgramLifecycleService");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down ProgramLifecycleService");
  }

  public RunId startProgram(final Id.Program id, final ProgramType programType,
                            final Map<String, String> userArgs, boolean debug) throws IOException {
    Program program = store.loadProgram(id, programType);
    if (program == null) {
      throw new FileNotFoundException(String.format("Program not found: Id = %s; Type = %s", id, programType));
    }

    BasicArguments userArguments = new BasicArguments(userArgs);
    Map<String, String> basicSystemArgs = getBasicSystemArguments(id.getNamespace());
    BasicArguments systemArguments = new BasicArguments(basicSystemArgs);
    ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.run(program, new SimpleProgramOptions(
      id.getId(), systemArguments, userArguments, debug));

    final ProgramController controller = runtimeInfo.getController();
    final String runId = controller.getRunId().getId();
    if (programType != ProgramType.MAPREDUCE) {
      // MapReduce state recording is done by the MapReduceProgramRunner
      // TODO [JIRA: CDAP-2013] Same needs to be done for other programs as well
      controller.addListener(new AbstractListener() {
        @Override
        public void init(ProgramController.State state, @Nullable Throwable cause) {
          // Get start time from RunId
          long startTimeInSeconds = RunIds.getTime(controller.getRunId(), TimeUnit.SECONDS);
          if (startTimeInSeconds == -1) {
            // If RunId is not time-based, use current time as start time
            startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
          }
          store.setStart(id, runId, startTimeInSeconds);
          if (state == ProgramController.State.COMPLETED) {
            completed();
          }
          if (state == ProgramController.State.ERROR) {
            error(controller.getFailureCause());
          }
        }

        @Override
        public void completed() {
          store.setStop(id, runId, TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                        ProgramController.State.COMPLETED.getRunStatus());
        }

        @Override
        public void killed() {
          store.setStop(id, runId, TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                        ProgramController.State.KILLED.getRunStatus());
        }

        @Override
        public void suspended() {
          store.setSuspend(id, runId);
        }

        @Override
        public void resuming() {
          store.setResume(id, runId);
        }

        @Override
        public void error(Throwable cause) {
          LOG.info("Program stopped with error {}, {}", id, runId, cause);
          store.setStop(id, runId, TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS),
                        ProgramController.State.ERROR.getRunStatus());
        }
      }, Threads.SAME_THREAD_EXECUTOR);
    }
    return controller.getRunId();
  }

  private Map<String, String> getBasicSystemArguments(Id.Namespace namespaceId) {
    Map<String, String> systemArgs = Maps.newHashMap();
    systemArgs.put(Constants.AppFabric.APP_SCHEDULER_QUEUE, queueResolver.getQueue(namespaceId));
    return systemArgs;
  }

  public void stopProgram(RunId runId) throws ExecutionException, InterruptedException {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.lookup(runId);
    runtimeInfo.getController().stop().get();
  }
}
