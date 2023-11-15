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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link OperationRuntime} implementation when operations are running inside same service.
 */
public class InMemoryOperationRuntime implements OperationRuntime {

  private final CConfiguration cConf;
  private final ConcurrentHashMap<OperationRunId, OperationController> controllers;
  private final OperationRunner runner;
  private final OperationStatePublisher statePublisher;
  private final TransactionRunner transactionRunner;

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryOperationRuntime.class);

  @Inject
  InMemoryOperationRuntime(CConfiguration cConf, OperationRunner runner,
      OperationStatePublisher statePublisher,
      TransactionRunner transactionRunner) {
    this.cConf = cConf;
    this.runner = runner;
    this.statePublisher = statePublisher;
    this.transactionRunner = transactionRunner;
    this.controllers = new ConcurrentHashMap<>();
  }

  /**
   * Runs an operation given the detail.
   *
   * @param runDetail detail of the run
   * @return {@link OperationController} for the run
   */
  public OperationController run(OperationRunDetail runDetail) {
    return controllers.computeIfAbsent(
        runDetail.getRunId(),
        runId -> {
          try {
            OperationController controller = runner.run(runDetail);
            LOG.debug("Added controller for {}", runId);
            controller.complete().addListener(() -> remove(runId), Threads.SAME_THREAD_EXECUTOR);
            return controller;
          } catch (IllegalStateException e) {
            statePublisher.publishFailed(runDetail.getRunId(),
                new OperationError(e.getMessage(), Collections.emptyList())
            );
          }
          return null;
        }
    );
  }

  /**
   * Get controller for a given {@link OperationRunId}.
   */
  @Nullable
  public OperationController getController(OperationRunDetail detail) {
    return controllers.computeIfAbsent(detail.getRunId(), runId -> {
      // TODO(samik) create controller from detail after 6.10 release
      return null;
    });
  }

  private void remove(OperationRunId runId) {
    OperationController controller = controllers.remove(runId);
    if (controller != null) {
      LOG.debug("Controller removed for {}", runId);
    }
  }

}
