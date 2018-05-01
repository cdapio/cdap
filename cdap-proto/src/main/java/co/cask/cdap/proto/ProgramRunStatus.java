/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package co.cask.cdap.proto;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.workflow.NodeStatus;

/**
 * Program Status Types used to query program runs
 */
public enum ProgramRunStatus {
  ALL,
  PENDING,
  STARTING,
  RUNNING,
  SUSPENDED,
  RESUMING,
  COMPLETED,
  FAILED,
  KILLED;

  /**
   * Return whether this state can transition to the specified state.
   *
   * @param status the state to transition to
   * @return whether this state can transition to the specified state
   */
  public boolean canTransitionTo(ProgramRunStatus status) {
    if (this == status) {
      return true;
    }
    switch (this) {
      case PENDING:
        // STARTING is the happy path
        // KILLED happens if the run was manually stopped
        // FAILED happens if the provisioning failed
        return status == STARTING || status == KILLED || status == FAILED;
      case STARTING:
        // RUNNING is the happy path
        // KILLED happens if the run was manually stopped
        // FAILED happens if the run failed while starting
        // COMPLETED happens somehow? Not sure when we expect this but we test that this transition can happen
        // SUSPENDED happens if you suspend while starting. Not sure why this is allowed, seems wrong (CDAP-13551)
        return status == RUNNING || status == SUSPENDED || status == COMPLETED || status == KILLED || status == FAILED;
      case RUNNING:
        // SUSPENDED happens if the run was suspended
        // COMPLETED is the happy path
        // KILLED happens if the run was manually stopped
        // FAILED happens if the run failed
        return status == SUSPENDED || status == COMPLETED || status == KILLED || status == FAILED;
      case SUSPENDED:
        // RUNNING happens if the run was resumed (there is no RESUMING state even though it is an enum value...)
        // KILLED happens if the run was manually stopped
        // FAILED happens if the run failed while suspended
        return status == RUNNING || status == KILLED || status == FAILED;
      case COMPLETED:
      case FAILED:
      case KILLED:
        // these are end states
        return false;
    }
    // these are not actually states, should never ask about transitioning
    throw new IllegalStateException("Invalid transition from program run state " + this);
  }

  /**
   * @return whether the status is an end status for a program run.
   */
  public boolean isEndState() {
    return this == COMPLETED || this == FAILED || this == KILLED;
  }

  /**
   * Conversion from program run status to Workflow node status.
   * @param status the program run status to be converted
   * @return the converted Workflow node status
   */
  public static NodeStatus toNodeStatus(ProgramRunStatus status) {
    switch(status) {
      case STARTING:
        return NodeStatus.STARTING;
      case RUNNING:
        return NodeStatus.RUNNING;
      case COMPLETED:
        return NodeStatus.COMPLETED;
      case FAILED:
        return NodeStatus.FAILED;
      case KILLED:
        return NodeStatus.KILLED;
      default:
        throw new IllegalArgumentException(String.format("No node status available corresponding to program status %s",
                                                         status.name()));
    }
  }

  public static ProgramStatus toProgramStatus(ProgramRunStatus status) {
    switch(status) {
      case PENDING:
      case STARTING:
        return ProgramStatus.INITIALIZING;
      case RUNNING:
        return ProgramStatus.RUNNING;
      case COMPLETED:
        return ProgramStatus.COMPLETED;
      case FAILED:
        return ProgramStatus.FAILED;
      case KILLED:
        return ProgramStatus.KILLED;
      default:
        throw new IllegalArgumentException(String.format("No program status available corresponding to program run " +
                                                         "status %s", status.name()));
    }
  }
}
