/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.queue;

import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.proto.Notification;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A scheduled job.
 */
public interface Job {

  /**
   * State of the job. The lifecycle of State is as follows:
   *
   *   *-----------------*       *--------------------*         *----------------*
   *   | PENDING_TRIGGER |  ---> | PENDING_CONSTRAINT |  --->   | PENDING_LAUNCH |
   *   *-----------------*       *--------------------*         *----------------*
   */
  enum State {
    /**
     * The Job is awaiting fulfillment of its Trigger.
     */
    PENDING_LAUNCH(Collections.<State>emptySet()),
    /**
     * The Job is awaiting satisfaction of all of its constraints.
     */
    PENDING_CONSTRAINT(Collections.singleton(PENDING_LAUNCH)),
    /**
     * The Job is awaiting launch.
     */
    PENDING_TRIGGER(Collections.singleton(PENDING_CONSTRAINT));

    private final Set<State> allowedNextStates;

    State(Set<State> allowedNextStates) {
      this.allowedNextStates = allowedNextStates;
    }

    /**
     * Checks that the specified State is a valid transition from the current state.
     */
    void checkTransition(State nextState) {
      Preconditions.checkArgument(allowedNextStates.contains(nextState),
                                  "Invalid Job State transition from '%s' to '%s'.", this, nextState);
    }
  }

  /**
   * Returns the {@link ProgramSchedule} associated with this Job.
   */
  ProgramSchedule getSchedule();

  /**
   * Returns the last modification time of the schedule. It represents the schedule at the time this job was created.
   */
  long getScheduleLastUpdatedTime();

  /**
   * Returns the creation time of this Job.
   */
  long getCreationTime();

  /**
   * Returns the list of {@link Notification}s associated with this Job.
   */
  List<Notification> getNotifications();

  /**
   * Returns the {@link State} of this Job.
   */
  State getState();

  /**
   * Returns the {@link JobKey} of this Job.
   */
  JobKey getJobKey();

  /**
   * Whether the job is marked for deletion.
   */
  boolean isToBeDeleted();

  /**
   * @return the time at which this job was marked for deletion, null only if {@link #isToBeDeleted} returns false
   */
  @Nullable
  Long getDeleteTimeMillis();
}

