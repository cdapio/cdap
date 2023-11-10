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

package io.cdap.cdap.proto.operation;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Status of operation run.
 */
public enum OperationRunStatus {
  PENDING,
  STARTING,
  RUNNING,
  STOPPING,
  SUCCEEDED,
  FAILED,
  KILLED;

  private static final Set<OperationRunStatus> UNSUCCESSFUL_STATES = EnumSet.of(FAILED, KILLED);
  private static final Set<OperationRunStatus> END_STATES = EnumSet.of(SUCCEEDED, FAILED, KILLED);
  private static final Set<String> END_STATE_NAMES =
      END_STATES.stream().map(OperationRunStatus::name).collect(Collectors.toSet());

  /**
   * Return whether this state can transition to the specified state.
   *
   * @param status the state to transition to
   * @return whether this state can transition to the specified state
   */
  public boolean canTransitionTo(OperationRunStatus status) {
    if (this == status) {
      return true;
    }
    switch (this) {
      case PENDING:
        return status == STARTING || status == STOPPING;
      case STARTING:
        // RUNNING is the happy path
        // STOPPING happens if the run was manually stopped gracefully(may include a timeout)
        // KILLED happens if the run was manually stopped
        // FAILED happens if the run failed while starting
        return status == RUNNING || status == STOPPING || status == KILLED || status == FAILED;
      case RUNNING:
        // STOPPING happens if the run was manually stopped (may include a graceful timeout)
        // SUCCEEDED is the happy path
        // FAILED happens if the run failed
        // KILLED happens if the run was manually stopped
        return status == STOPPING || status == SUCCEEDED || status == KILLED || status == FAILED;
      case STOPPING:
        // SUCCEEDED is the happy path
        // KILLED happens if the run was manually stopped
        // FAILED happens if the run failed
        return status == SUCCEEDED || status == KILLED || status == FAILED;
      case SUCCEEDED:
      case FAILED:
      case KILLED:
        // these are end states
        return false;
      default:
        throw new IllegalStateException("Invalid transition from program run state " + this);
    }
  }

  /**
   * Checks whether the status is an end status for a program run.
   */
  public boolean isEndState() {
    return END_STATES.contains(this);
  }

  /**
   * Checks whether a name (string) represents an end status for a program run.
   */
  public static boolean isEndState(String name) {
    return END_STATE_NAMES.contains(name);
  }

  /**
   * Checks whether the status is an end status for a program run.
   */
  public boolean isUnsuccessful() {
    return UNSUCCESSFUL_STATES.contains(this);
  }
}
