/*
 * Copyright © 2017-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule.constraint;

import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProtoConstraint;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
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
    Map<ProgramRunId, RunRecordDetail> activeRuns = context.getActiveRuns(schedule.getProgramId());
    if (activeRuns.size() >= maxConcurrency) {
      LOG.debug("Skipping run of program {} from schedule {} because there are {} active runs.",
                schedule.getProgramId(), schedule.getName(), activeRuns.size());
      return notSatisfied(context);
    }
    return ConstraintResult.SATISFIED;
  }

  private ConstraintResult notSatisfied(ConstraintContext context) {
    if (!waitUntilMet) {
      return ConstraintResult.NEVER_SATISFIED;
    }
    return new ConstraintResult(ConstraintResult.SatisfiedState.NOT_SATISFIED,
                                context.getCheckTimeMillis() + TimeUnit.SECONDS.toMillis(10));
  }
}
