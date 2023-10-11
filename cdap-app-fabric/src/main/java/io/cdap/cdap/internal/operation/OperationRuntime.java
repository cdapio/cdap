/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.operation;


import com.google.inject.Inject;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides management of running operations. Stores {@link OperationController} in memory.
 */
public class OperationRuntime {

  private final ReadWriteLock runtimeLock;
  private final Map<OperationRunId, OperationController> controllers;
  private final OperationRunner runner;
  private final OperationStatePublisher statePublisher;

  private static final Logger LOG = LoggerFactory.getLogger(OperationRuntime.class);

  @Inject
  OperationRuntime(OperationRunner runner, OperationStatePublisher statePublisher) {
    this.runner = runner;
    this.statePublisher = statePublisher;
    this.runtimeLock = new ReentrantReadWriteLock();
    this.controllers = new HashMap<>();
  }

  public final OperationController run(OperationRunDetail runDetail) {
    OperationController controller = getController(runDetail.getRunId());
    if (controller != null) {
      LOG.debug("Operation is already running: {}", runDetail.getRunId());
      return controller;
    }
    try {
      updateController(runDetail.getRunId(), runner.run(runDetail));
    } catch (OperationTypeNotSupportedException e) {
      statePublisher.publishFailed(runDetail.getRunId(),
          new OperationError(e.getMessage(), Collections.emptyList())
      );
    }
    return controllers.get(runDetail.getRunId());
  }

  @Nullable
  public OperationController getController(OperationRunId runId) {
    Lock lock = this.runtimeLock.readLock();
    lock.lock();
    try {
      return controllers.get(runId);
      // TODO(samik) fetch from store for remote operations
    } finally {
      lock.unlock();
    }
  }

  /**
   * Updates the controller cache by adding the given {@link OperationController} if it does not
   * exist.
   */
  void updateController(OperationRunId runId, OperationController controller) {
    // Add the runtime info if it does not exist in the cache.
    Lock lock = this.runtimeLock.writeLock();
    lock.lock();
    try {
      controllers.put(runId, controller);
    } finally {
      lock.unlock();
    }

    LOG.debug("Added controller for {}", runId);
    controller.completionFuture().addListener(() -> remove(runId), Threads.SAME_THREAD_EXECUTOR);
  }

  private void remove(OperationRunId runId) {
    OperationController controller;
    Lock lock = this.runtimeLock.writeLock();
    lock.lock();
    try {
      controller = controllers.remove(runId);
    } finally {
      lock.unlock();
    }

    if (controller != null) {
      LOG.debug("Controller removed for {}", runId);
    }
  }

  /**
   * Returns {@code true} if the operation is running.
   */
  protected boolean isRunning(OperationRunId runId) {
    return getController(runId) != null;
  }
}
