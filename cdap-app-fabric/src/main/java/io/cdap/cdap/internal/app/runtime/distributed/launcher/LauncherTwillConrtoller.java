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

package io.cdap.cdap.internal.app.runtime.distributed.launcher;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
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
import javax.annotation.Nullable;

/**
 *
 */
public class LauncherTwillConrtoller implements TwillController {

  private static final Logger LOG = LoggerFactory.getLogger(LauncherTwillConrtoller.class);

  private final RunId runId;
  private final CompletableFuture<LauncherTwillConrtoller> started;
  private final CompletableFuture<LauncherTwillConrtoller> completion;

  LauncherTwillConrtoller(RunId runId) {
    this.runId = runId;

    CompletableFuture<LauncherTwillConrtoller> completion = new CompletableFuture<>();
    this.completion = completion;
    this.started = new CompletableFuture<>();
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
    // TODO (CDAP-13403): Use runtime monitor to change
    return Futures.immediateFailedFuture(new UnsupportedOperationException("updateLogLevels is not supported"));
  }

  @Override
  public Future<Map<String, LogEntry.Level>> updateLogLevels(String runnableName,
                                                             Map<String, LogEntry.Level> logLevelsForRunnable) {
    // TODO (CDAP-13403): Use runtime monitor to change
    return Futures.immediateFailedFuture(new UnsupportedOperationException("updateLogLevels is not supported"));
  }

  @Override
  public Future<String[]> resetLogLevels(String... loggerNames) {
    // TODO (CDAP-13403): Use runtime monitor to change
    return Futures.immediateFailedFuture(new UnsupportedOperationException("resetLogLevels is not supported"));
  }

  @Override
  public Future<String[]> resetRunnableLogLevels(String runnableName, String... loggerNames) {
    // TODO (CDAP-13403): Use runtime monitor to change
    return Futures.immediateFailedFuture(new UnsupportedOperationException("resetRunnableLogLevels is not supported"));
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public Future<Command> sendCommand(Command command) {
    // TODO (CDAP-13403): Use runtime monitor to send command
    return Futures.immediateFailedFuture(new UnsupportedOperationException("sendCommand is not supported"));
  }

  @Override
  public Future<Command> sendCommand(String runnableName, Command command) {
    // TODO (CDAP-13403): Use runtime monitor to send command
    return Futures.immediateFailedFuture(new UnsupportedOperationException("sendCommand is not supported"));
  }

  @Override
  public Future<? extends ServiceController> terminate() {
    return completion;
  }

  @Override
  public void kill() {
    // TODO (CDAP-13403): ssh to kill the remote process, followed by publishing a KILLED state would force kill the run
    try {
      terminate().get();
    } catch (InterruptedException | ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void onRunning(Runnable runnable, Executor executor) {
    started.thenRunAsync(runnable, executor);
  }

  @Override
  public void onTerminated(Runnable runnable, Executor executor) {
    completion.whenCompleteAsync((controller, throwable) -> runnable.run(), executor);
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
    // TODO (CDAP-13403): The actual state needs to be coming from the runtime monitor
    try {
      awaitTerminated();
      return TerminationStatus.SUCCEEDED;
    } catch (ExecutionException e) {
      return TerminationStatus.FAILED;
    }
  }
}
