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

package co.cask.cdap.internal.provision.task;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.provision.ProvisionerTable;
import co.cask.cdap.internal.provision.ProvisioningOp;
import co.cask.cdap.internal.provision.ProvisioningTaskInfo;
import co.cask.cdap.internal.provision.ProvisioningTaskKey;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.provisioner.RetryableProvisionException;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A Provisioning task that is responsible for executing multiple subtasks. Before each subtask is executed, state
 * will be persisted to the ProvisionerTable so that it can be picked up later in case the task is interrupted
 * partway through.
 *
 * Handles retrying any subtasks that throw a RetryableProvisioningException.
 */
public abstract class ProvisioningTask {
  private static final Logger LOG = LoggerFactory.getLogger(ProvisioningTask.class);
  private final Transactional transactional;
  private final DatasetFramework datasetFramework;
  private final ProvisioningTaskKey taskKey;
  protected final ProvisioningTaskInfo initialTaskInfo;
  protected final ProgramRunId programRunId;
  protected final int retryTimeLimitSecs;

  public ProvisioningTask(ProvisioningTaskInfo initialTaskInfo, Transactional transactional,
                          DatasetFramework datasetFramework, int retryTimeLimitSecs) {
    this.taskKey = new ProvisioningTaskKey(initialTaskInfo.getProgramRunId(),
                                           initialTaskInfo.getProvisioningOp().getType());
    this.programRunId = initialTaskInfo.getProgramRunId();
    this.initialTaskInfo = initialTaskInfo;
    this.transactional = transactional;
    this.datasetFramework = datasetFramework;
    this.retryTimeLimitSecs = retryTimeLimitSecs;
  }

  /**
   * Execute all subtasks, persisting task info before each subtask such that this task can be re-created from the
   * task info stored in the ProvisionerTable.
   *
   * @throws InterruptedException if the task was interrupted
   * @throws TransactionFailureException if there was a failure persisting task info
   * @throws Exception if there was a non-retryable exception while executing a subtask
   */
  public void execute() throws Exception {
    LOG.debug("Starting {} task for program run {}.", initialTaskInfo.getProvisioningOp().getType(), programRunId);
    RetryStrategy retryStrategy =
      RetryStrategies.statefulTimeLimit(retryTimeLimitSecs, TimeUnit.SECONDS, System.currentTimeMillis(),
                                        RetryStrategies.exponentialDelay(100, 20000, TimeUnit.MILLISECONDS));
    Map<ProvisioningOp.Status, ProvisioningSubtask> subtasks = getSubtasks();

    Optional<ProvisioningTaskInfo> taskInfoOptional = Optional.of(initialTaskInfo);
    while (taskInfoOptional.isPresent()) {
      ProvisioningTaskInfo nextTaskInfo = taskInfoOptional.get();
      ProvisioningTaskInfo taskInfo = persistTaskInfo(nextTaskInfo, retryStrategy);

      ProvisioningOp.Status state = taskInfo.getProvisioningOp().getStatus();
      if (state == ProvisioningOp.Status.CANCELLED) {
        return;
      }

      ProvisioningSubtask subtask = subtasks.get(state);
      if (subtask == null) {
        // should never happen
        throw new IllegalStateException(
          String.format("Invalid state '%s' in provisioning task for program run '%s'. "
                          + "This means there is a bug in provisioning state machine. "
                          + "Please reach out to the development team.",
                        state, programRunId));
      }
      if (subtask == EndSubtask.INSTANCE) {
        break;
      }

      try {
        LOG.debug("Executing {} subtask {} for program run {}.",
                  taskInfo.getProvisioningOp().getType(), state, programRunId);
        taskInfoOptional = Retries.callWithInterruptibleRetries(() -> subtask.execute(taskInfo), retryStrategy,
                                                                t -> t instanceof RetryableProvisionException);
        LOG.debug("Completed {} subtask {} for program run {}.",
                  taskInfo.getProvisioningOp().getType(), state, programRunId);
      } catch (InterruptedException e) {
        throw e;
      } catch (Exception e) {
        LOG.error("{} task failed in {} state for program run {}.",
                  taskInfo.getProvisioningOp().getType(), state, programRunId, e);
        handleSubtaskFailure(taskInfo, e);
        ProvisioningOp failureOp = new ProvisioningOp(taskInfo.getProvisioningOp().getType(),
                                                      ProvisioningOp.Status.FAILED);
        ProvisioningTaskInfo failureInfo = new ProvisioningTaskInfo(taskInfo, failureOp, taskInfo.getCluster());
        persistTaskInfo(failureInfo, retryStrategy);
        return;
      }
    }
    LOG.debug("Completed {} task for program run {}.", initialTaskInfo.getProvisioningOp().getType(), programRunId);
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
  protected ProvisioningTaskInfo persistTaskInfo(ProvisioningTaskInfo taskInfo, RetryStrategy retryStrategy)
    throws InterruptedException {
    try {
      // Stop retrying if we are interrupted. Otherwise, retry on every exception, up to the retry limit
      return Retries.callWithInterruptibleRetries(() -> Transactionals.execute(transactional, dsContext -> {
        ProvisionerTable dataset = ProvisionerTable.get(dsContext, datasetFramework);
        ProvisioningTaskInfo currentState = dataset.getTaskInfo(taskKey);
        // if the state is cancelled, don't write anything and transition to the end subtask.
        if (currentState != null && currentState.getProvisioningOp().getStatus() == ProvisioningOp.Status.CANCELLED) {
          return currentState;
        }
        dataset.putTaskInfo(taskInfo);
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
   * @return subtasks that make up this task.
   */
  protected abstract Map<ProvisioningOp.Status, ProvisioningSubtask> getSubtasks();

  /**
   * Logic to run when a subtask fails in a non-retryable way.
   *
   * @param taskInfo task info for the failure
   * @param e the non-retryable exception
   */
  protected abstract void handleSubtaskFailure(ProvisioningTaskInfo taskInfo, Exception e);

  /**
   * Logic to run when task info could not be saved to the ProvisionerTable.
   *
   * @param taskInfo task info that could not be saved
   * @param e the non-retryable exception
   */
  protected abstract void handleStateSaveFailure(ProvisioningTaskInfo taskInfo, Exception e);
}
