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


import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.cdap.cdap.common.TooManyRequestsException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.proto.operation.OperationType;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link OperationRuntime} implementation when operations are running inside same service.
 */
public class InMemoryOperationRuntime implements OperationRuntime {

  private final ConcurrentHashMap<OperationRunId, OperationController> controllers;
  private final ConcurrentHashMap<OperationType, AtomicInteger> operationCounters;
  private final ImmutableMap<OperationType, Integer> maxOperationCounts;
  private final OperationRunner runner;
  private final OperationStatePublisher statePublisher;

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryOperationRuntime.class);

  @Inject
  InMemoryOperationRuntime(OperationRunner runner, OperationStatePublisher statePublisher,
      CConfiguration cConf) {
    this.runner = runner;
    this.statePublisher = statePublisher;
    this.controllers = new ConcurrentHashMap<>();
    this.operationCounters = new ConcurrentHashMap<>();
    this.maxOperationCounts = getMaxCountsFromCconf(cConf);
  }

  /**
   * Runs an operation given the detail.
   *
   * @param runDetail detail of the run
   * @return {@link OperationController} for the run
   */
  public OperationController run(OperationRunDetail runDetail) {
    OperationType type = runDetail.getRun().getType();
    AtomicInteger counter = operationCounters.computeIfAbsent(
        runDetail.getRun().getType(),
        t -> new AtomicInteger()
    );
    return controllers.computeIfAbsent(
        runDetail.getRunId(),
        runId -> {
          try {
            if (counter.incrementAndGet() > maxOperationCounts.get(type) && maxOperationCounts.get(type) > 0) {
              counter.decrementAndGet();
              throw new TooManyRequestsException(
                  String.format("Maximum number of %s operations allowed is %d", type,
                      maxOperationCounts.get(type))
              );
            }
            OperationController controller = runner.run(runDetail);
            LOG.debug("Added controller for {}", runId);
            controller.complete()
                .addListener(() -> remove(runId, counter), Threads.SAME_THREAD_EXECUTOR);
            return controller;
          } catch (IllegalStateException | TooManyRequestsException e) {
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

  private void remove(OperationRunId runId, AtomicInteger counter) {
    OperationController controller = controllers.remove(runId);
    if (controller != null) {
      LOG.debug("Controller removed for {}", runId);
    }
    if (counter != null) {
      counter.decrementAndGet();
    }
  }

  private ImmutableMap<OperationType, Integer> getMaxCountsFromCconf(CConfiguration cConf) {
    ImmutableMap.Builder<OperationType, Integer> builder = ImmutableMap.builder();
    for (OperationType type : OperationType.values()) {
      String maxCountConfString = String.format("operation.%s.max.count",
          type.name().toLowerCase());
      builder.put(type, cConf.getInt(maxCountConfString, -1));
    }
    return builder.build();
  }

}
