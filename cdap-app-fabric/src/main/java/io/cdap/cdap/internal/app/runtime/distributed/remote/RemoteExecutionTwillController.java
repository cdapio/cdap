/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.Command;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceController;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Implementation of {@link TwillController} that uses {@link RemoteProcessController} to control a running program.
 */
class RemoteExecutionTwillController implements TwillController {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutionTwillController.class);

  private final ProgramRunId programRunId;
  private final RunId runId;
  private final CompletionStage<TwillController> started;
  private final CompletableFuture<TwillController> completion;
  private final ScheduledExecutorService scheduler;
  private final RemoteProcessController remoteProcessController;
  private final RemoteExecutionService executionService;
  private final long gracefulShutdownMillis;
  private final long pollCompletedMillis;
  private volatile boolean terminateOnServiceStop;

  RemoteExecutionTwillController(CConfiguration cConf, ProgramRunId programRunId,
                                 CompletionStage<?> startupCompletionStage,
                                 RemoteProcessController remoteProcessController,
                                 ScheduledExecutorService scheduler, RemoteExecutionService service) {
    this.programRunId = programRunId;
    this.runId = RunIds.fromString(programRunId.getRun());
    this.gracefulShutdownMillis = cConf.getLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS);
    this.pollCompletedMillis = cConf.getLong(Constants.RuntimeMonitor.POLL_TIME_MS);

    // On start up task succeeded, complete the started stage to unblock the onRunning()
    // On start up task failure, mark this controller as terminated with exception
    CompletableFuture<TwillController> completion = new CompletableFuture<>();
    this.started = startupCompletionStage.thenApply(o -> RemoteExecutionTwillController.this);
    this.terminateOnServiceStop = true;
    this.started.exceptionally(throwable -> {
      completion.completeExceptionally(throwable);
      return RemoteExecutionTwillController.this;
    });
    service.addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        if (terminateOnServiceStop) {
          completion.complete(RemoteExecutionTwillController.this);
        }
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        if (terminateOnServiceStop) {
          completion.completeExceptionally(failure);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    this.completion = completion;
    this.scheduler = scheduler;
    this.remoteProcessController = remoteProcessController;
    this.executionService = service;
  }

  public void release() {
    terminateOnServiceStop = false;
    executionService.stop();
  }

  public void complete() {
    terminateOnServiceStop = true;
    executionService.stop();
  }

  @Override
  public Future<? extends ServiceController> terminate() {
    if (completion.isDone()) {
      return CompletableFuture.completedFuture(this);
    }

    CompletableFuture<TwillController> result = completion.thenApply(r -> r);
    scheduler.execute(() -> {
      try {
        remoteProcessController.terminate();

        // Poll for completion
        long killTimeMillis = System.currentTimeMillis() + gracefulShutdownMillis + pollCompletedMillis * 5;
        scheduler.schedule(new Runnable() {
          @Override
          public void run() {
            try {
              if (!remoteProcessController.isRunning()) {
                completion.complete(RemoteExecutionTwillController.this);
                return;
              }
              // If the process is still running, kills it if it reaches the kille time.
              if (System.currentTimeMillis() >= killTimeMillis) {
                remoteProcessController.kill();
                completion.complete(RemoteExecutionTwillController.this);
                return;
              }

              // Schedule to check again
              scheduler.schedule(this, pollCompletedMillis, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
              result.completeExceptionally(e);
            }
          }
        }, pollCompletedMillis, TimeUnit.MILLISECONDS);

      } catch (Exception e) {
        // Only fail the result future. We have to keep the terminationFuture to be not completed so that the
        // caller can retry termination.
        result.completeExceptionally(e);
      }
    });
    return result;
  }

  @Override
  public void kill() {
    try {
      remoteProcessController.kill();
    } catch (Exception e) {
      throw new RuntimeException("Failed when requesting program " + programRunId + " to stop", e);
    }
    try {
      Uninterruptibles.getUninterruptibly(completion);
    } catch (ExecutionException e) {
      // We ignore termination error since we only care about killing the program, but not interested in the final state
      LOG.debug("Exception raised when terminating program {}", programRunId, e);
    }
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
