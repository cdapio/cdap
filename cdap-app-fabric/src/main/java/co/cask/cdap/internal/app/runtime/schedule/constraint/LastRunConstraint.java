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

package co.cask.cdap.internal.app.runtime.schedule.constraint;

import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProtoConstraint;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.concurrent.TimeUnit;

/**
 * A Constraint which requires a certain duration pass since the last execution of the schedule.
 */
public class LastRunConstraint extends ProtoConstraint.LastRunConstraint implements CheckableConstraint {

  public LastRunConstraint(long millisSinceLastRun) {
    super(millisSinceLastRun);
  }

  @Override
  public ConstraintResult check(ProgramSchedule schedule, ConstraintContext context) {
    long startTime = context.getCheckTime() - millisSinceLastRun;
    // We only need to check program runs within recent history, adding a buffer of 1 day, because the time range
    // is for the start time of the program. It may start before `millisSinceLastRun`, but complete after it.
    // Note: this will miss out on active workflow runs that started more than ~1day ago (suspended/lengthy workflows)
    Iterable<RunRecordMeta> runRecords =
      context.getProgramRuns(schedule.getProgramId(), ProgramRunStatus.ALL,
                             startTime - TimeUnit.DAYS.toMillis(1), Long.MAX_VALUE, 100).values();
    // We can limit to 100, since just 1 program in the recent history is enough to make the constraint fail.
    // We want use 100 as the limit instead of 1, because we want to attempt to get the latest completed run,
    // instead of just the first (in order to more accurately compute a next runtime


    if (Iterables.isEmpty(filter(runRecords, startTime))) {
      return ConstraintResult.SATISFIED;
    }
    if (!waitUntilMet) {
      return ConstraintResult.NEVER_SATISFIED;
    }
    return new ConstraintResult(ConstraintResult.SatisfiedState.NOT_SATISFIED, TimeUnit.SECONDS.toMillis(10));
  }

  // Filters run records that are: FAILED, KILLED; keeps only: RUNNING, SUSPENDED, COMPLETED.
  // Also filters COMLPETED run records that have completed before the specified startTime.
  private Iterable<RunRecordMeta> filter(Iterable<RunRecordMeta> runRecords, final long startTime) {
    return Iterables.filter(runRecords, new Predicate<RunRecordMeta>() {
      @Override
      public boolean apply(RunRecordMeta input) {
        if (ProgramRunStatus.COMPLETED == input.getStatus() && input.getStopTs() < startTime) {
          return false;
        }
        // otherwise, simply check that its not a FAILED/KILLED run
        return ProgramRunStatus.FAILED != input.getStatus() && ProgramRunStatus.KILLED != input.getStatus();
      }
    });
  }
}
