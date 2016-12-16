/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.guice;

import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * A {@link TwillController} wrapper that performs impersonation on {@link #getResourceReport()}, {@link #terminate()}
 * and {@link #kill()}.
 */
final class ImpersonatedTwillController implements TwillController {

  private static final Logger LOG = LoggerFactory.getLogger(ImpersonatedTwillController.class);

  private final TwillController delegate;
  private final Impersonator impersonator;
  private final ProgramId programId;

  ImpersonatedTwillController(TwillController delegate, Impersonator impersonator, ProgramId programId) {
    this.delegate = delegate;
    this.impersonator = impersonator;
    this.programId = programId;
  }

  @Override
  public void addLogHandler(LogHandler handler) {
    delegate.addLogHandler(handler);
  }

  @Override
  public ServiceDiscovered discoverService(String serviceName) {
    return delegate.discoverService(serviceName);
  }

  @Override
  public Future<Integer> changeInstances(String runnable, int newCount) {
    return delegate.changeInstances(runnable, newCount);
  }

  @Nullable
  @Override
  public ResourceReport getResourceReport() {
    try {
      return impersonator.doAs(programId.getNamespaceId(), new Callable<ResourceReport>() {
        @Nullable
        @Override
        public ResourceReport call() throws Exception {
          return delegate.getResourceReport();
        }
      });
    } catch (Exception e) {
      // The delegate.getResourceReport() call never throws exception, hence exception caught must due to
      // impersonation failure.
      if (Throwables.getRootCause(e) instanceof ServiceUnavailableException) {
        // If it is due to some underlying service unavailability, log a debug message and return null
        // It is expected to happen during master process startup
        LOG.debug("Failed in impersonation for program {}", programId, e);
      } else {
        LOG.warn("Unexpected exception in impersonation for program {}", programId, e);
      }
      return null;
    }
  }

  @Override
  public Future<String> restartAllInstances(String runnable) {
    return delegate.restartAllInstances(runnable);
  }

  @Override
  public Future<Set<String>> restartInstances(Map<String, ? extends Set<Integer>> runnableToInstanceIds) {
    return delegate.restartInstances(runnableToInstanceIds);
  }

  @Override
  public Future<String> restartInstances(String runnable, int instanceId, int... moreInstanceIds) {
    return delegate.restartInstances(runnable, instanceId, moreInstanceIds);
  }

  @Override
  public Future<Map<String, LogEntry.Level>> updateLogLevels(Map<String, LogEntry.Level> logLevels) {
    return delegate.updateLogLevels(logLevels);
  }

  @Override
  public Future<Map<String, LogEntry.Level>> updateLogLevels(String runnableName,
                                                             Map<String, LogEntry.Level> logLevels) {
    return delegate.updateLogLevels(runnableName, logLevels);
  }

  @Override
  public Future<String[]> resetLogLevels(String... loggerNames) {
    return delegate.resetLogLevels(loggerNames);
  }

  @Override
  public Future<String[]> resetRunnableLogLevels(String runnableName, String... loggerNames) {
    return delegate.resetRunnableLogLevels(runnableName, loggerNames);
  }

  @Override
  public RunId getRunId() {
    return delegate.getRunId();
  }

  @Override
  public Future<Command> sendCommand(Command command) {
    return delegate.sendCommand(command);
  }

  @Override
  public Future<Command> sendCommand(String runnableName, Command command) {
    return delegate.sendCommand(runnableName, command);
  }

  @Override
  public Future<? extends ServiceController> terminate() {
    try {
      return impersonator.doAs(programId.getNamespaceId(), new Callable<Future<? extends ServiceController>>() {
        @Override
        public Future<? extends ServiceController> call() throws Exception {
          return delegate.terminate();
        }
      });
    } catch (Exception e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  @Override
  public void kill() {
    try {
      impersonator.doAs(programId.getNamespaceId(), new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          delegate.kill();
          return null;
        }
      });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void onRunning(Runnable runnable, Executor executor) {
    delegate.onRunning(runnable, executor);
  }

  @Override
  public void onTerminated(Runnable runnable, Executor executor) {
    delegate.onTerminated(runnable, executor);
  }

  @Override
  public void awaitTerminated() throws ExecutionException {
    delegate.awaitTerminated();
  }

  @Override
  public void awaitTerminated(long timeout, TimeUnit timeoutUnit) throws TimeoutException, ExecutionException {
    delegate.awaitTerminated(timeout, timeoutUnit);
  }
}
