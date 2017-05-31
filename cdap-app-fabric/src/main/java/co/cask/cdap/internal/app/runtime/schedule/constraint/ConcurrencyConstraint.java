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
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProtoConstraint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A constraint which dictates an upper bound on the number of concurrent schedule runs.
 */
public class ConcurrencyConstraint extends ProtoConstraint.ConcurrencyConstraint implements CheckableConstraint {
  private static final Logger LOG = LoggerFactory.getLogger(ConcurrencyConstraint.class);

  public ConcurrencyConstraint(int maxConcurrency) {
    super(maxConcurrency);
  }

  @Override
  public ConstraintResult check(ProgramSchedule schedule, ConstraintContext context) {
    int numRunning = context.getProgramRuns(schedule.getProgramId(), ProgramRunStatus.RUNNING, maxConcurrency).size();
    if (numRunning >= maxConcurrency) {
        LOG.debug("Skipping run of program {} from schedule {} because there are at least {} running runs.",
                  schedule.getProgramId(), schedule.getName(), maxConcurrency);
      return notSatisfied();
    }

    int numSuspended =
      context.getProgramRuns(schedule.getProgramId(), ProgramRunStatus.SUSPENDED, maxConcurrency).size();
    if (numRunning + numSuspended >= maxConcurrency) {
        LOG.debug("Skipping run of program {} from schedule {} because there are at least" +
                    "{} running runs and at least {} suspended runs.",
                  schedule.getProgramId(), schedule.getName(), numRunning, numSuspended);
      return notSatisfied();
    }
    return ConstraintResult.SATISFIED;
  }

  private ConstraintResult notSatisfied() {
    if (!waitUntilMet) {
      return ConstraintResult.NEVER_SATISFIED;
    }
    return new ConstraintResult(ConstraintResult.SatisfiedState.NOT_SATISFIED, TimeUnit.SECONDS.toMillis(10));
  }
}
