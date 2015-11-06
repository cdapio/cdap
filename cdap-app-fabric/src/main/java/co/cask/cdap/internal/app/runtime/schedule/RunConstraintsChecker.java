/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import com.google.common.base.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Checks whether all schedule run constraints are met for the schedule to run.
 */
class RunConstraintsChecker {
  private static final Logger LOG = LoggerFactory.getLogger(RunConstraintsChecker.class);

  private final Store store;

  RunConstraintsChecker(Store store) {
    this.store = store;
  }

  /**
   * Checks if the run requirements for the specified schedule and program are all satisfied.
   *
   * @param programId the id of the program to check
   * @param schedule the schedule to check
   * @return whether all run requirements are satisfied
   */
  public boolean checkSatisfied(Id.Program programId, Schedule schedule) {
    String scheduleName = schedule.getName();
    Integer maxRuns = schedule.getRunConstraints().getMaxConcurrentRuns();
    Predicate<RunRecordMeta> scheduleFilter = getScheduleFilter(scheduleName);
    if (maxRuns != null) {
      try {
        int numActive = 0;
        List<RunRecordMeta> runs = store.getRuns(programId, ProgramRunStatus.RUNNING, 0, Long.MAX_VALUE, maxRuns,
                                                 scheduleFilter);
        numActive += runs.size();
        runs = store.getRuns(programId, ProgramRunStatus.SUSPENDED, 0, Long.MAX_VALUE, maxRuns,
                             scheduleFilter);
        numActive += runs.size();

        if (numActive >= maxRuns) {
          LOG.info("Skipping run of program {} because there are already {} active runs of schedule {}.",
                   programId, maxRuns, schedule.getName());
          return false;
        }
      } catch (Exception e) {
        LOG.error("Exception looking up active runs of program {}. Skipping scheduled run.",
          programId, e);
        // if we couldn't look up from the store, something bad is happening so we probably shouldn't launch a run.
        return false;
      }
    }
    return true;
  }

  private Predicate<RunRecordMeta> getScheduleFilter(final String scheduleName) {
    return new Predicate<RunRecordMeta>() {
      @Override
      public boolean apply(RunRecordMeta input) {
        Map<String, String> systemArgs = input.getSystemArgs();
        if (systemArgs != null) {
          String runScheduleName = systemArgs.get(ProgramOptionConstants.SCHEDULE_NAME);
          return scheduleName.equals(runScheduleName);
        }
        return false;
      }
    };
  }
}
