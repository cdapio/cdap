/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.runtimejob;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.runtimejob.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import org.apache.twill.api.Command;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceController;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * Implementation of {@link TwillController} that uses {@link RuntimeJobManager} to monitor and
 * control a running program.
 */
class RuntimeJobTwillController implements TwillController {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeJobTwillController.class);

  private final RunId runId;
  private final ProgramRunId programRunId;
  private final RuntimeJobManager jobManager;
  private final CompletableFuture<RuntimeJobTwillController> started;
  private final CompletableFuture<RuntimeJobTwillController> completion;
  private final AtomicBoolean terminateCalled;

  RuntimeJobTwillController(RuntimeJobManager jobManager, ProgramRunId programRunId) {
    this.programRunId = programRunId;
    this.runId = RunIds.fromString(programRunId.getRun());
    this.jobManager = jobManager;
    this.started = new CompletableFuture<>();
    this.completion = new CompletableFuture<>();
    this.terminateCalled = new AtomicBoolean();
  }

  RuntimeJobManager getJobManager() {
    return jobManager;
  }

  void start(CompletableFuture<Void> startupTaskCompletion) {
    startupTaskCompletion.whenComplete((res, throwable) -> {
      // terminate this controller with fail state
      if (throwable == null) {
        started.complete(this);
      } else {
        completion.completeExceptionally(throwable);
      }
    });
  }

  @Override
  public void addLogHandler(LogHandler handler) {
    LOG.trace("LogHandler is not supported for {}", getClass().getSimpleName());
  }

  @Override
  public ServiceDiscovered discoverService(String serviceName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<Integer> changeInstances(String runnable, int newCount) {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public ResourceReport getResourceReport() {
    return null;
  }

  @Override
  public Future<String> restartAllInstances(String runnable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<Set<String>> restartInstances(Map<String, ? extends Set<Integer>> runnableToInstanceIds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<String> restartInstances(String runnable, int instanceId, int... moreInstanceIds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<String> restartInstances(String runnable, Set<Integer> instanceIds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<Map<String, LogEntry.Level>> updateLogLevels(Map<String, LogEntry.Level> logLevels) {
    return Futures.immediateFailedFuture(new UnsupportedOperationException("updateLogLevels is not supported"));
  }

  @Override
  public Future<Map<String, LogEntry.Level>> updateLogLevels(String runnableName,
                                                             Map<String, LogEntry.Level> logLevelsForRunnable) {
    return Futures.immediateFailedFuture(new UnsupportedOperationException("updateLogLevels is not supported"));
  }

  @Override
  public Future<String[]> resetLogLevels(String... loggerNames) {
    return Futures.immediateFailedFuture(new UnsupportedOperationException("resetLogLevels is not supported"));
  }

  @Override
  public Future<String[]> resetRunnableLogLevels(String runnableName, String... loggerNames) {
    return Futures.immediateFailedFuture(new UnsupportedOperationException("resetRunnableLogLevels is not supported"));
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public Future<Command> sendCommand(Command command) {
    return Futures.immediateFailedFuture(new UnsupportedOperationException("sendCommand is not supported"));
  }

  @Override
  public Future<Command> sendCommand(String runnableName, Command command) {
    return Futures.immediateFailedFuture(new UnsupportedOperationException("sendCommand is not supported"));
  }

  @Override
  public void onRunning(Runnable runnable, Executor executor) {
    started.thenRunAsync(runnable, executor);
  }

  @Override
  public void onTerminated(Runnable runnable, Executor executor) {
    completion.whenCompleteAsync((remoteExecutionTwillController, throwable) -> runnable.run(), executor);
  }

  @Override
  public void awaitTerminated() throws ExecutionException {
    Uninterruptibles.getUninterruptibly(completion);
  }

  @Override
  public void awaitTerminated(long timeout, TimeUnit timeoutUnit) throws TimeoutException, ExecutionException {
    Uninterruptibles.getUninterruptibly(completion, timeout, timeoutUnit);
  }

  @Override
  public Future<? extends ServiceController> terminate() {
    if (terminateCalled.compareAndSet(false, true)) {
      try {
        // stop the job
        jobManager.stop(new ProgramRunInfo.Builder()
                          .setNamespace(programRunId.getNamespace())
                          .setApplication(programRunId.getApplication())
                          .setProgram(programRunId.getProgram())
                          .setProgramType(programRunId.getType().getPrettyName())
                          .setRun(programRunId.getRun()).build());
        // mark completion as completed
        completion.complete(this);
      } catch (Exception e) {
        completion.completeExceptionally(e);
      }
    }
    return completion;
  }

  @Override
  public void kill() {
    try {
      terminate().get();
    } catch (InterruptedException | ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  @Nullable
  @Override
  public TerminationStatus getTerminationStatus() {
    if (!completion.isDone()) {
      return null;
    }

    try {
      awaitTerminated();
      return TerminationStatus.SUCCEEDED;
    } catch (ExecutionException e) {
      return TerminationStatus.FAILED;
    }
  }
}
