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

package co.cask.cdap.internal.app.runtime.distributed.remote;

import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitor;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Implementation of {@link TwillController} that uses {@link RuntimeMonitor} to monitor and control a running
 * program.
 */
public class RemoteExecutionTwillController implements TwillController {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutionTwillController.class);

  private final RunId runId;
  private final RuntimeMonitor runtimeMonitor;
  private final CompletableFuture<RemoteExecutionTwillController> completion;

  public RemoteExecutionTwillController(RunId runId, RuntimeMonitor runtimeMonitor) {
    this.runId = runId;
    this.runtimeMonitor = runtimeMonitor;

    CompletableFuture<RemoteExecutionTwillController> completion = new CompletableFuture<>();
    runtimeMonitor.addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        completion.complete(RemoteExecutionTwillController.this);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        completion.completeExceptionally(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    this.completion = completion;
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
    runtimeMonitor.stop();
    return completion;
  }

  @Override
  public void kill() {
    // TODO (CDAP-13288): Runtime monitor should have a kill call
    runtimeMonitor.stop();
  }

  @Override
  public void onRunning(Runnable runnable, Executor executor) {
    executor.execute(runnable);
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
    // TODO (CDAP-13403): The actual state needs to be coming from the runtime monitor
    try {
      awaitTerminated();
      return TerminationStatus.SUCCEEDED;
    } catch (ExecutionException e) {
      return TerminationStatus.FAILED;
    }
  }
}
