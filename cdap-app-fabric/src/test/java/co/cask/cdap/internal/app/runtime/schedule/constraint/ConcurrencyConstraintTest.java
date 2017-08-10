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

import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.queue.Job;
import co.cask.cdap.internal.app.runtime.schedule.queue.SimpleJob;
import co.cask.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ConcurrencyConstraint}.
 */
public class ConcurrencyConstraintTest {
  private static final NamespaceId TEST_NS = new NamespaceId("ConcurrencyConstraintTest");
  private static final ApplicationId APP_ID = TEST_NS.app("app1");
  private static final WorkflowId WORKFLOW_ID = APP_ID.workflow("wf1");
  private static final DatasetId DATASET_ID = TEST_NS.dataset("pfs1");

  private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();

  @Test
  public void testMaxConcurrentRuns() {
    Store store = AppFabricTestHelper.getInjector().getInstance(Store.class);

    long now = System.currentTimeMillis();
    ProgramSchedule schedule = new ProgramSchedule("SCHED1", "one partition schedule", WORKFLOW_ID,
                                                   ImmutableMap.of("prop3", "abc"),
                                                   new PartitionTrigger(DATASET_ID, 1),
                                                   ImmutableList.<Constraint>of());
    SimpleJob job = new SimpleJob(schedule, now, Collections.<Notification>emptyList(), Job.State.PENDING_TRIGGER, 0L);

    ConcurrencyConstraint concurrencyConstraint = new ConcurrencyConstraint(2);
    ConstraintContext constraintContext = new ConstraintContext(job, now, store);

    assertSatisfied(true, concurrencyConstraint.check(schedule, constraintContext));

    String pid1 = RunIds.generate().getId();
    String pid2 = RunIds.generate().getId();
    String pid3 = RunIds.generate().getId();

    // add a run for the schedule
    Map<String, String> systemArgs = ImmutableMap.of(ProgramOptionConstants.SCHEDULE_NAME, schedule.getName());
    store.setStart(WORKFLOW_ID, pid1, System.currentTimeMillis(), null, EMPTY_MAP, systemArgs);
    assertSatisfied(true, concurrencyConstraint.check(schedule, constraintContext));

    // add a run for the program from a different schedule. Since there are now 2 running instances of the
    // workflow (regardless of the schedule name), the constraint is not met
    systemArgs = ImmutableMap.of(ProgramOptionConstants.SCHEDULE_NAME, "not" + schedule.getName());
    store.setStart(WORKFLOW_ID, pid2, System.currentTimeMillis(), null, EMPTY_MAP, systemArgs);
    assertSatisfied(false, concurrencyConstraint.check(schedule, constraintContext));

    // add a run for the program that wasn't from a schedule
    // there are now three concurrent runs, so the constraint will not be met
    store.setStart(WORKFLOW_ID, pid3, System.currentTimeMillis(), null, EMPTY_MAP, EMPTY_MAP);
    assertSatisfied(false, concurrencyConstraint.check(schedule, constraintContext));

    // stop the first program; constraint will not be satisfied as there are still 2 running
    store.setStop(WORKFLOW_ID, pid1, System.currentTimeMillis(), ProgramRunStatus.COMPLETED);
    assertSatisfied(false, concurrencyConstraint.check(schedule, constraintContext));

    // suspending/resuming the workflow doesn't reduce its concurrency count
    store.setSuspend(WORKFLOW_ID, pid3);
    assertSatisfied(false, concurrencyConstraint.check(schedule, constraintContext));
    store.setResume(WORKFLOW_ID, pid3);
    assertSatisfied(false, concurrencyConstraint.check(schedule, constraintContext));

    // but the constraint will be satisfied with it completes, as there is only 1 remaining RUNNING
    store.setStop(WORKFLOW_ID, pid3, System.currentTimeMillis(), ProgramRunStatus.KILLED);
    assertSatisfied(true, concurrencyConstraint.check(schedule, constraintContext));

    // stopping the last running workflow will also satisfy the constraint
    store.setStop(WORKFLOW_ID, pid2, System.currentTimeMillis(), ProgramRunStatus.FAILED);
    assertSatisfied(true, concurrencyConstraint.check(schedule, constraintContext));
  }

  private void assertSatisfied(boolean expectSatisfied, ConstraintResult constraintResult) {
    if (expectSatisfied) {
      Assert.assertEquals(ConstraintResult.SATISFIED, constraintResult);
    } else {
      Assert.assertNotEquals(ConstraintResult.SATISFIED, constraintResult.getSatisfiedState());
    }
  }
}
