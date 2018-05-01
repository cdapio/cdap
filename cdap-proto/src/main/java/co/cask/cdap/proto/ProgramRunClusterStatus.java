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
 */

package co.cask.cdap.proto;

/**
 * State of the cluster for a program run
 */
public enum ProgramRunClusterStatus {
  PROVISIONING,
  PROVISIONED,
  WAITING,
  DEPROVISIONING,
  DEPROVISIONED,
  ORPHANED;

  /**
   * Return whether this state can transition to the specified state.
   *
   * @param status the state to transition to
   * @return whether this state can transition to the specified state
   */
  public boolean canTransitionTo(ProgramRunClusterStatus status) {
    if (this == status) {
      return true;
    }
    switch (this) {
      case PROVISIONING:
        // PROVISIONED is the happy path
        // DEPROVISIONING happens if there was an error after trying to create the cluster
        // DEPROVISIONED happens if there was an error prior to trying to create the cluster
        // ORPHANED happens if the cluster was created but got into a bad state and could not be deleted
        return status == PROVISIONED || status == DEPROVISIONING || status == DEPROVISIONED || status == ORPHANED;
      case PROVISIONED:
        // WAITING if the run fails or is stopped and there is an expiry set to keep the cluster around for some time
        // DEPROVISIONING if the run completes or is stopped and there is no expiry set
        return status == WAITING || status == DEPROVISIONING;
      case WAITING:
        // DEPROVISIONING after the expiry is met, or the run is manually deprovisioned
        // ORPHANED if it cannot be deprovisioned. For example, the provisioner was removed.
        return status == DEPROVISIONING || status == ORPHANED;
      case DEPROVISIONING:
        // DEPROVISIONED is the happy path
        // ORPHANED if the cluster delete failed in a non-retryable fashion
        return status == DEPROVISIONED || status == ORPHANED;
      case DEPROVISIONED:
      case ORPHANED:
        // these are end states
        return false;
    }
    // should never get here
    throw new IllegalStateException("Unimplemented cluster state " + this);
  }
}
