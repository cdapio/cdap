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
import co.cask.cdap.internal.app.runtime.schedule.queue.Job;
import co.cask.cdap.internal.app.runtime.schedule.queue.SimpleJob;
import co.cask.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link DelayConstraint}.
 */
public class DelayConstraintTest {

  private static final NamespaceId TEST_NS = new NamespaceId("DelayConstraintTest");
  private static final ApplicationId APP_ID = TEST_NS.app("app1");
  private static final WorkflowId WORKFLOW_ID = APP_ID.workflow("wf1");
  private static final DatasetId DATASET_ID = TEST_NS.dataset("pfs1");

  @Test
  public void test() {
    long now = System.currentTimeMillis();
    ProgramSchedule schedule = new ProgramSchedule("SCHED1", "one partition schedule", WORKFLOW_ID,
                                                   ImmutableMap.of("prop3", "abc"),
                                                   new PartitionTrigger(DATASET_ID, 1),
                                                   ImmutableList.<Constraint>of());
    SimpleJob job = new SimpleJob(schedule, now, Collections.<Notification>emptyList(), Job.State.PENDING_TRIGGER, 0L);

    // test with 10 minute delay
    DelayConstraint tenMinuteDelayConstraint = new DelayConstraint(TimeUnit.MINUTES.toMillis(10));
    // a check against 12 minutes after 'now' will return SATISFIED
    ConstraintResult result =
      tenMinuteDelayConstraint.check(schedule, new ConstraintContext(job, now + TimeUnit.MINUTES.toMillis(12)));
    Assert.assertEquals(ConstraintResult.SATISFIED, result);

    // a check against 9 minutes after 'now' will return NOT_SATISFIED, with 1 minute to wait until next retry
    result = tenMinuteDelayConstraint.check(schedule, new ConstraintContext(job, now + TimeUnit.MINUTES.toMillis(9)));
    Assert.assertEquals(ConstraintResult.SatisfiedState.NOT_SATISFIED, result.getSatisfiedState());
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(1), (long) result.getMillisBeforeNextRetry());
  }

}
