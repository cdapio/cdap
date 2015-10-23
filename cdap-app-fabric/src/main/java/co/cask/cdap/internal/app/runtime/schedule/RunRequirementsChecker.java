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
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Checks whether all schedule requirements are met for the schedule to run.
 */
public final class RunRequirementsChecker {
  private static final Logger LOG = LoggerFactory.getLogger(RunRequirementsChecker.class);

  private final Store store;

  RunRequirementsChecker(Store store) {
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
    Integer programThreshold = schedule.getRunRequirements().getConcurrentProgramRunsThreshold();
    if (programThreshold != null) {
      try {
        List<RunRecordMeta> running =
          store.getRuns(programId, ProgramRunStatus.RUNNING, 0, Long.MAX_VALUE, programThreshold + 1);
        if (running.size() > programThreshold) {
          LOG.info("Skipping run of program {} because there are over {} runs of the program in the RUNNING state.",
                   programId, programThreshold);
          return false;
        }
      } catch (Exception e) {
        LOG.error("Exception looking up runs of program {} in the RUNNING state. Skipping scheduled run.",
          programId, e);
        // if we couldn't look up from the store, something bad is happening so we probably shouldn't launch a run.
        return false;
      }
    }
    return true;
  }

}
