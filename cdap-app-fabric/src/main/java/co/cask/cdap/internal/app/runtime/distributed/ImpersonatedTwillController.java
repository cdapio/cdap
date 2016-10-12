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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import org.apache.twill.api.Command;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceController;
import org.apache.twill.api.TwillController;
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
public class ImpersonatedTwillController implements TwillController {

  private static final Logger LOG = LoggerFactory.getLogger(ImpersonatedTwillController.class);

  private final Impersonator impersonator;
  private final ProgramId programId;
  private final TwillController delegate;

  public ImpersonatedTwillController(Impersonator impersonator, ProgramId programId, TwillController delegate) {
    this.impersonator = impersonator;
    this.programId = programId;
    this.delegate = delegate;
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
        @Override
        public ResourceReport call() throws Exception {
          return delegate.getResourceReport();
        }
      });
    } catch (Exception e) {
      LOG.warn("Failed in getting resource report for {}.", programId, e);
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
