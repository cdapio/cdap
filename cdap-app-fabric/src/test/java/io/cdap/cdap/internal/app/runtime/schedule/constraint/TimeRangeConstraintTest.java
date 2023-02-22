/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.runtime.schedule.queue.SimpleJob;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramReference;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link TimeRangeConstraint}.
 */
public class TimeRangeConstraintTest {

  private static final NamespaceId TEST_NS = new NamespaceId("TimeRangeConstraintTest");
  private static final ApplicationId APP_ID = TEST_NS.app("app1");
  private static final ProgramReference WORKFLOW_REF = APP_ID.getAppReference().program(ProgramType.WORKFLOW,
                                                                                        "wf1");
  private static final DatasetId DATASET_ID = TEST_NS.dataset("pfs1");

  private static final ProgramSchedule SCHEDULE = new ProgramSchedule("SCHED1", "one partition schedule", WORKFLOW_REF,
                                                                      ImmutableMap.of("prop3", "abc"),
                                                                      new PartitionTrigger(DATASET_ID, 1),
                                                                      ImmutableList.<Constraint>of());

  @Test
  public void testInit() {
    TimeRangeConstraint timeRangeConstraint = new TimeRangeConstraint("16:00", "17:00", TimeZone.getTimeZone("PST"));
    Assert.assertEquals("PST", timeRangeConstraint.getTimeZone());

    // simple construction should work
    new TimeRangeConstraint("03:16", "03:17", TimeZone.getDefault());
    // leading zero shouldn't be necessary
    new TimeRangeConstraint("03:16", "3:17", TimeZone.getDefault());

    // start time can be a smaller numerical value than the end time
    new TimeRangeConstraint("22:30", "10:00", TimeZone.getDefault());

    // start time must not equal end time
    try {
      new TimeRangeConstraint("03:17", "03:17", TimeZone.getDefault());
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testForwardRange() {
    // 3:24PM PST
    long now = 1494368640000L;
    SimpleJob job = new SimpleJob(SCHEDULE, 0, now, Collections.<Notification>emptyList(),
                                  Job.State.PENDING_TRIGGER, 0L);

    // use a TimeRangeConstraint [4:00PM, 5:00PM)
    TimeRangeConstraint timeRangeConstraint = new TimeRangeConstraint("16:00", "17:00", TimeZone.getTimeZone("PST"));
    ConstraintContext constraintContext = createConstraintContext(job, now);
    ConstraintResult result = timeRangeConstraint.check(SCHEDULE, constraintContext);
    Assert.assertEquals(ConstraintResult.SatisfiedState.NOT_SATISFIED, result.getSatisfiedState());
    // 36 minutes till 4PM
    Assert.assertEquals(constraintContext.getCheckTimeMillis() + TimeUnit.MINUTES.toMillis(36),
                        (long) result.getNextCheckTime());

    constraintContext = createConstraintContext(job, result.getNextCheckTime() - 1);
    result = timeRangeConstraint.check(SCHEDULE, constraintContext);
    Assert.assertEquals(ConstraintResult.SatisfiedState.NOT_SATISFIED, result.getSatisfiedState());
    Assert.assertEquals(constraintContext.getCheckTimeMillis() + 1L, (long) result.getNextCheckTime());

    result = timeRangeConstraint.check(SCHEDULE, createConstraintContext(job, now + TimeUnit.MINUTES.toMillis(36)));
    Assert.assertEquals(ConstraintResult.SATISFIED, result);

    // 5:00PM PST
    long fivePM = 1494374400000L;
    constraintContext = createConstraintContext(job, fivePM);
    result = timeRangeConstraint.check(SCHEDULE, constraintContext);
    Assert.assertEquals(ConstraintResult.SatisfiedState.NOT_SATISFIED, result.getSatisfiedState());
    // 23 hours until the next time its 4PM again
    Assert.assertEquals(constraintContext.getCheckTimeMillis() + TimeUnit.HOURS.toMillis(23),
                        (long) result.getNextCheckTime());
  }

  @Test
  public void testReverseRange() {
    // 3:24PM PST
    long now = 1494368640000L;

    SimpleJob job = new SimpleJob(SCHEDULE, 0, now, Collections.<Notification>emptyList(),
                                  Job.State.PENDING_TRIGGER, 0L);

    // use a TimeRangeConstraint [10:00PM, 6:00AM)
    TimeRangeConstraint timeRangeConstraint = new TimeRangeConstraint("22:00", "06:00", TimeZone.getTimeZone("PST"));
    ConstraintContext constraintContext = createConstraintContext(job, now);
    ConstraintResult result = timeRangeConstraint.check(SCHEDULE, constraintContext);
    Assert.assertEquals(ConstraintResult.SatisfiedState.NOT_SATISFIED, result.getSatisfiedState());
    // 6 hours + 36 minutes till 4PM
    long sixHoursAnd36Minutes = TimeUnit.HOURS.toMillis(6) + TimeUnit.MINUTES.toMillis(36);
    Assert.assertEquals(constraintContext.getCheckTimeMillis() + sixHoursAnd36Minutes,
                        (long) result.getNextCheckTime());

    constraintContext = createConstraintContext(job, result.getNextCheckTime() - 1);
    result = timeRangeConstraint.check(SCHEDULE, constraintContext);
    Assert.assertEquals(ConstraintResult.SatisfiedState.NOT_SATISFIED, result.getSatisfiedState());
    Assert.assertEquals(constraintContext.getCheckTimeMillis() + 1L, (long) result.getNextCheckTime());

    result = timeRangeConstraint.check(SCHEDULE, createConstraintContext(job, now + sixHoursAnd36Minutes));
    Assert.assertEquals(ConstraintResult.SATISFIED, result);

    // 5:00PM PST
    long fivePM = 1494374400000L;
    constraintContext = createConstraintContext(job, fivePM);
    result = timeRangeConstraint.check(SCHEDULE, constraintContext);
    Assert.assertEquals(ConstraintResult.SatisfiedState.NOT_SATISFIED, result.getSatisfiedState());
    // 5 hours until the next time its 10PM again
    Assert.assertEquals(constraintContext.getCheckTimeMillis() + TimeUnit.HOURS.toMillis(5),
                        (long) result.getNextCheckTime());

    // 5:00AM PST
    long fiveAM = 1494331200000L;
    constraintContext = createConstraintContext(job, fiveAM);
    result = timeRangeConstraint.check(SCHEDULE, constraintContext);
    Assert.assertEquals(ConstraintResult.SatisfiedState.SATISFIED, result.getSatisfiedState());

    // 6:00AM PST - not satisfied, because the end range is exclusive
    long sixAM = 1494334800000L;
    constraintContext = createConstraintContext(job, sixAM);
    result = timeRangeConstraint.check(SCHEDULE, constraintContext);
    Assert.assertEquals(ConstraintResult.SatisfiedState.NOT_SATISFIED, result.getSatisfiedState());
    // 16 hours until the next time its 10PM
    Assert.assertEquals(constraintContext.getCheckTimeMillis() + TimeUnit.HOURS.toMillis(16),
                        (long) result.getNextCheckTime());

    // 7:00AM PST
    long sevenAM = 1494338400000L;
    constraintContext = createConstraintContext(job, sevenAM);
    result = timeRangeConstraint.check(SCHEDULE, constraintContext);
    Assert.assertEquals(ConstraintResult.SatisfiedState.NOT_SATISFIED, result.getSatisfiedState());
    // 15 hours until the next time its 10PM
    Assert.assertEquals(constraintContext.getCheckTimeMillis() + TimeUnit.HOURS.toMillis(15),
                        (long) result.getNextCheckTime());
  }

  private ConstraintContext createConstraintContext(Job job, long checkTime) {
    // should be fine to pass in null as the Store parameter, since its not used in this test
    return new ConstraintContext(job, checkTime, null);
  }

}
