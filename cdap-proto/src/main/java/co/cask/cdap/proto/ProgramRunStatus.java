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
  STARTING,
  RUNNING,
  SUSPENDED,
  RESUMING,
  COMPLETED,
  FAILED,
  KILLED;

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
