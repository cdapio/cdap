/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.common.twill;

import org.apache.twill.api.Command;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.internal.AbstractExecutionServiceController;
import org.apache.twill.internal.RunIds;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import javax.annotation.Nullable;

/**
 * A no-op {@link TwillController}.
 */
final class NoopTwillController extends AbstractExecutionServiceController implements TwillController {

  NoopTwillController() {
    super(RunIds.generate());
  }

  @Override
  public void addLogHandler(LogHandler handler) {
    // no-op
  }

  @Override
  public ServiceDiscovered discoverService(String serviceName) {
    return new ServiceDiscovered() {
      @Override
      public String getName() {
        return serviceName;
      }

      @Override
      public Cancellable watchChanges(ChangeListener listener, Executor executor) {
        return () -> { };
      }

      @Override
      public boolean contains(Discoverable discoverable) {
        return false;
      }

      @Override
      public Iterator<Discoverable> iterator() {
        return Collections.emptyIterator();
      }
    };
  }

  @Override
  public Future<Integer> changeInstances(String runnable, int newCount) {
    CompletableFuture<Integer> future = new CompletableFuture<>();
    future.completeExceptionally(new UnsupportedOperationException("Change instances is not supported"));
    return future;
  }

  @Nullable
  @Override
  public ResourceReport getResourceReport() {
    return null;
  }

  @Override
  public Future<String> restartAllInstances(String runnable) {
    CompletableFuture<String> future = new CompletableFuture<>();
    future.completeExceptionally(new UnsupportedOperationException("Restart all instances is not supported"));
    return future;
  }

  @Override
  public Future<Set<String>> restartInstances(Map<String, ? extends Set<Integer>> runnableToInstanceIds) {
    CompletableFuture<Set<String>> future = new CompletableFuture<>();
    future.completeExceptionally(new UnsupportedOperationException("Restart instances is not supported"));
    return future;
  }

  @Override
  public Future<String> restartInstances(String runnable, int instanceId, int... moreInstanceIds) {
    CompletableFuture<String> future = new CompletableFuture<>();
    future.completeExceptionally(new UnsupportedOperationException("Restart instances is not supported"));
    return future;
  }

  @Override
  public Future<String> restartInstances(String runnable, Set<Integer> instanceIds) {
    CompletableFuture<String> future = new CompletableFuture<>();
    future.completeExceptionally(new UnsupportedOperationException("Restart instances is not supported"));
    return future;
  }

  @Override
  public Future<Map<String, LogEntry.Level>> updateLogLevels(Map<String, LogEntry.Level> logLevels) {
    CompletableFuture<Map<String, LogEntry.Level>> future = new CompletableFuture<>();
    future.completeExceptionally(new UnsupportedOperationException("Update log levels is not supported"));
    return future;
  }

  @Override
  public Future<Map<String, LogEntry.Level>> updateLogLevels(String runnableName,
                                                             Map<String, LogEntry.Level> logLevelsForRunnable) {
    CompletableFuture<Map<String, LogEntry.Level>> future = new CompletableFuture<>();
    future.completeExceptionally(new UnsupportedOperationException("Update log levels is not supported"));
    return future;
  }

  @Override
  public Future<String[]> resetLogLevels(String... loggerNames) {
    CompletableFuture<String[]> future = new CompletableFuture<>();
    future.completeExceptionally(new UnsupportedOperationException("Reset log levels is not supported"));
    return future;
  }

  @Override
  public Future<String[]> resetRunnableLogLevels(String runnableName, String... loggerNames) {
    CompletableFuture<String[]> future = new CompletableFuture<>();
    future.completeExceptionally(new UnsupportedOperationException("Reset runnable log levels is not supported"));
    return future;
  }

  @Override
  protected void startUp() {
    // no-op
  }

  @Override
  protected void shutDown() {
    // no-op
  }

  @Override
  public Future<Command> sendCommand(Command command) {
    return CompletableFuture.completedFuture(command);
  }

  @Override
  public Future<Command> sendCommand(String runnableName, Command command) {
    return CompletableFuture.completedFuture(command);
  }

  @Override
  public void kill() {
    terminate();
  }
}
