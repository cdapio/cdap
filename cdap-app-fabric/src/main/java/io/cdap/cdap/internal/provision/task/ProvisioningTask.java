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
 *
 */

package io.cdap.cdap.internal.provision.task;

import io.cdap.cdap.common.async.RepeatedTask;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.provision.ProvisionerTable;
import io.cdap.cdap.internal.provision.ProvisioningOp;
import io.cdap.cdap.internal.provision.ProvisioningTaskInfo;
import io.cdap.cdap.internal.provision.ProvisioningTaskKey;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategy;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.RetryableProvisionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A Provisioning task that is responsible for executing multiple subtasks. Before each subtask is executed, state
 * will be persisted to the ProvisionerStore so that it can be picked up later in case the task is interrupted
 * partway through.
 *
 * Handles retrying any subtasks that throw a RetryableProvisioningException.
 */
public abstract class ProvisioningTask implements RepeatedTask {

  private static final Logger LOG = LoggerFactory.getLogger(ProvisioningTask.class);

  protected final ProgramRunId programRunId;
  protected final int retryTimeLimitSecs;
  protected final Provisioner provisioner;
  protected final ProvisionerContext provisionerContext;


  private final TransactionRunner transactionRunner;
  private final ProvisioningTaskKey taskKey;
  private final ProvisioningTaskInfo initialTaskInfo;

  private ProvisioningTaskInfo taskInfo;
  private RetryStrategy retryStrategy;
  private Map<ProvisioningOp.Status, ProvisioningSubtask> subTasks;
  private PollingStrategy subTaskPollingStrategy;
  private long subTaskStartTime;
  private int subTaskExecNums;

  protected ProvisioningTask(Provisioner provisioner, ProvisionerContext provisionerContext,
                             ProvisioningTaskInfo initialTaskInfo, TransactionRunner transactionRunner,
                             int retryTimeLimitSecs) {
    this.provisioner = provisioner;
    this.provisionerContext = provisionerContext;
    this.initialTaskInfo = initialTaskInfo;
    this.taskKey = new ProvisioningTaskKey(initialTaskInfo.getProgramRunId(),
                                           initialTaskInfo.getProvisioningOp().getType());
    this.programRunId = initialTaskInfo.getProgramRunId();
    this.taskInfo = initialTaskInfo;
    this.transactionRunner = transactionRunner;
    this.retryTimeLimitSecs = retryTimeLimitSecs;

    LOG.debug("Created {} task for program run {}.", initialTaskInfo.getProvisioningOp().getType(), programRunId);
  }

  /**
   * Executes one iteration of subtask. It persists task info before each subtask such that this task
   * can be re-created from the task info stored in the ProvisionerStore.
   */
  @Override
  public final long executeOnce() throws Exception {
    RetryStrategy retryStrategy = getRetryStrategy();
    Map<ProvisioningOp.Status, ProvisioningSubtask> subTasks = getSubTasks();
    ProvisioningTaskInfo currentTaskInfo = persistTaskInfo(taskInfo, retryStrategy);

    ProvisioningOp.Status state = currentTaskInfo.getProvisioningOp().getStatus();
    if (state == ProvisioningOp.Status.CANCELLED) {
      LOG.debug("Cancelled {} task for program run {}.", initialTaskInfo.getProvisioningOp().getType(), programRunId);
      return -1L;
    }

    // Get the sub-task to execute
    ProvisioningSubtask subtask = subTasks.get(state);
    if (subtask == null) {
      // should never happen
      throw new IllegalStateException(
        String.format("Invalid state '%s' in provisioning task for program run '%s'. "
                        + "This means there is a bug in provisioning state machine. "
                        + "Please reach out to the development team.",
                      state, programRunId));
    }
    if (subtask == EndSubtask.INSTANCE) {
      LOG.debug("Completed {} task for program run {}.", initialTaskInfo.getProvisioningOp().getType(), programRunId);
      return -1L;
    }

    if (subTaskStartTime == 0L) {
      subTaskStartTime = System.currentTimeMillis();
    }

    try {
      LOG.debug("Executing {} subtask {} for program run {}.",
                currentTaskInfo.getProvisioningOp().getType(), state, programRunId);
      taskInfo = Retries.callWithInterruptibleRetries(() -> subtask.execute(currentTaskInfo), retryStrategy,
                                                      t -> t instanceof RetryableProvisionException).orElse(null);
      LOG.debug("Completed {} subtask {} for program run {}.",
                currentTaskInfo.getProvisioningOp().getType(), state, programRunId);

      // Nothing more to execute
      if (taskInfo == null) {
        LOG.debug("No more {} task for program run {}.", initialTaskInfo.getProvisioningOp().getType(), programRunId);
        return -1L;
      }

      ProvisioningOp.Status nextState = taskInfo.getProvisioningOp().getStatus();

      // If state doesn't change, determine the delay based on the polling strategy
      if (state == nextState) {
        if (subTaskPollingStrategy == null) {
          subTaskPollingStrategy = provisioner.getPollingStrategy(provisionerContext, taskInfo.getCluster());
        }
        return Math.max(0, subTaskPollingStrategy.nextPoll(subTaskExecNums++, subTaskStartTime));
      }
      // Otherwise, execute the next task immediately.
      subTaskPollingStrategy = null;
      subTaskStartTime = 0L;
      subTaskExecNums = 0;
      return 0;
    } catch (InterruptedException e) {
      throw e;
    } catch (Exception e) {
      LOG.error("{} task failed in {} state for program run {}.",
                currentTaskInfo.getProvisioningOp().getType(), state, programRunId, e);
      handleSubtaskFailure(currentTaskInfo, e);
      ProvisioningOp failureOp = new ProvisioningOp(currentTaskInfo.getProvisioningOp().getType(),
                                                    ProvisioningOp.Status.FAILED);
      ProvisioningTaskInfo failureInfo = new ProvisioningTaskInfo(currentTaskInfo, failureOp,
                                                                  currentTaskInfo.getCluster());
      persistTaskInfo(failureInfo, retryStrategy);
      LOG.debug("Terminated {} task for program run {} due to exception.",
                initialTaskInfo.getProvisioningOp().getType(), programRunId);
      return -1L;
    }
  }

  /**
   * Write the task state to the {@link ProvisionerTable}, retrying if any exception is caught. Before persisting
   * the state, the current state will be checked. If the current state is cancelled, it will not be overwritten.
   *
   * @param taskInfo the task state to save
   * @param retryStrategy the retry strategy to use on errors
   * @return the task info that is stored. This will be the taskInfo that was given to this method unless the existing
   *   task info was in the cancelled state, in which case the cancelled info will be returned.
   * @throws InterruptedException if we were interrupted while waiting between retries
   * @throws RuntimeException if there was an error and the retry limit was hit
   */
  private ProvisioningTaskInfo persistTaskInfo(ProvisioningTaskInfo taskInfo,
                                               RetryStrategy retryStrategy) throws InterruptedException {
    try {
      // Stop retrying if we are interrupted. Otherwise, retry on every exception, up to the retry limit
      return Retries.callWithInterruptibleRetries(() -> TransactionRunners.run(transactionRunner, context -> {
        ProvisionerTable provisionerTable = new ProvisionerTable(context);
        ProvisioningTaskInfo currentState = provisionerTable.getTaskInfo(taskKey);
        // if the state is cancelled, don't write anything and transition to the end subtask.
        if (currentState != null && currentState.getProvisioningOp().getStatus() == ProvisioningOp.Status.CANCELLED) {
          return currentState;
        }
        provisionerTable.putTaskInfo(taskInfo);
        return taskInfo;
      }), retryStrategy, t -> true);
    } catch (RuntimeException e) {
      LOG.error("{} task failed in to save state for {} subtask. The task will be failed.",
                taskInfo.getProvisioningOp().getType(), taskInfo.getProvisioningOp().getStatus(), e);
      // this is thrown if we ran out of retries
      handleStateSaveFailure(taskInfo, e);
      throw e;
    }
  }

  /**
   * Get the subtasks that make up this task. Each map key represents a state in a state machine. Each subtask
   * is responsible for executing any logic that should occur in that state, as well as providing the next state in
   * the state machine. This task will execute by getting the subtask for the current state from this map,
   * executing the subtask, getting the next state from the subtask, then looking up the next subtask from this map.
   * This will loop until a subtask is executed that does not have a next state.
   *
   * @param initialInfo the initial {@link ProvisioningTaskInfo} for this task
   * @return subtasks that make up this task.
   */
  protected abstract Map<ProvisioningOp.Status, ProvisioningSubtask> createSubTasks(ProvisioningTaskInfo initialInfo);

  /**
   * Logic to run when a subtask fails in a non-retryable way.
   *
   * @param taskInfo task info for the failure
   * @param e the non-retryable exception
   */
  protected abstract void handleSubtaskFailure(ProvisioningTaskInfo taskInfo, Exception e);

  /**
   * Logic to run when task info could not be saved to the ProvisionerStore.
   *
   * @param taskInfo task info that could not be saved
   * @param e the non-retryable exception
   */
  protected abstract void handleStateSaveFailure(ProvisioningTaskInfo taskInfo, Exception e);

  /**
   * Returns the {@link RetryStrategy} for the task execution
   */
  private RetryStrategy getRetryStrategy() {
    if (retryStrategy == null) {
      retryStrategy = RetryStrategies.statefulTimeLimit(retryTimeLimitSecs, TimeUnit.SECONDS,
                                                        System.currentTimeMillis(),
                                                        RetryStrategies.exponentialDelay(100, 20000,
                                                                                         TimeUnit.MILLISECONDS));
    }
    return retryStrategy;
  }

  /**
   * Returns the {@link Map} that maps {@link ProvisioningOp.Status} to {@link ProvisioningSubtask} for sub-task
   * execution.
   */
  private Map<ProvisioningOp.Status, ProvisioningSubtask> getSubTasks() {
    if (subTasks == null) {
      subTasks = createSubTasks(initialTaskInfo);
    }
    return subTasks;
  }
}
