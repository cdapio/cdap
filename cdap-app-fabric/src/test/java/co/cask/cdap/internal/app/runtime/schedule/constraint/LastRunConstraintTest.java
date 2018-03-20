/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link LastRunConstraint}.
 */
public class LastRunConstraintTest {
  private static final NamespaceId TEST_NS = new NamespaceId("LastRunConstraintTest");
  private static final ApplicationId APP_ID = TEST_NS.app("app1");
  private static final WorkflowId WORKFLOW_ID = APP_ID.workflow("wf1");
  private static final DatasetId DATASET_ID = TEST_NS.dataset("pfs1");

  private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();
  private int sourceId;

  private void setStartAndRunning(Store store, final ProgramRunId id, final long startTime) {
    setStartAndRunning(store, id, startTime, ImmutableMap.of(), ImmutableMap.of());
  }

  private void setStartAndRunning(Store store, final ProgramRunId id, final long startTime,
                                  final Map<String, String> runtimeArgs,
                                  final Map<String, String> systemArgs) {
    store.setProvisioning(id, startTime, runtimeArgs, systemArgs, AppFabricTestHelper.createSourceId(++sourceId));
    store.setProvisioned(id, 0, AppFabricTestHelper.createSourceId(++sourceId));
    store.setStart(id, null, systemArgs, AppFabricTestHelper.createSourceId(++sourceId));
    store.setRunning(id, startTime + 1, null, AppFabricTestHelper.createSourceId(++sourceId));
  }
  
  @Test
  public void testLastRunConstraint() {
    Store store = AppFabricTestHelper.getInjector().getInstance(Store.class);

    long now = System.currentTimeMillis();
    long nowSec = TimeUnit.MILLISECONDS.toSeconds(now);
    ProgramSchedule schedule = new ProgramSchedule("SCHED1", "one partition schedule", WORKFLOW_ID,
                                                   ImmutableMap.of("prop3", "abc"),
                                                   new PartitionTrigger(DATASET_ID, 1),
                                                   ImmutableList.of());
    SimpleJob job = new SimpleJob(schedule, now, Collections.emptyList(), Job.State.PENDING_TRIGGER, 0L);

    // require 1 hour since last run
    LastRunConstraint lastRunConstraint = new LastRunConstraint(1, TimeUnit.HOURS);
    ConstraintContext constraintContext = new ConstraintContext(job, now, store);

    // there's been no runs, so the constraint is satisfied by default
    assertSatisfied(true, lastRunConstraint.check(schedule, constraintContext));


    ProgramRunId pid1 = WORKFLOW_ID.run(RunIds.generate().getId());
    ProgramRunId pid2 = WORKFLOW_ID.run(RunIds.generate().getId());
    ProgramRunId pid3 = WORKFLOW_ID.run(RunIds.generate().getId());
    ProgramRunId pid4 = WORKFLOW_ID.run(RunIds.generate().getId());

    // a RUNNING workflow, started 3 hours ago will fail the constraint check
    Map<String, String> systemArgs = ImmutableMap.of(ProgramOptionConstants.SCHEDULE_NAME, schedule.getName());
    long startTime = nowSec - TimeUnit.HOURS.toSeconds(3);
    setStartAndRunning(store, pid1, startTime, EMPTY_MAP, systemArgs);
    assertSatisfied(false, lastRunConstraint.check(schedule, constraintContext));

    // a SUSPENDED workflow started 3 hours ago will also fail the constraint check
    store.setSuspend(pid1, AppFabricTestHelper.createSourceId(++sourceId));
    assertSatisfied(false, lastRunConstraint.check(schedule, constraintContext));
    store.setResume(pid1, AppFabricTestHelper.createSourceId(++sourceId));
    assertSatisfied(false, lastRunConstraint.check(schedule, constraintContext));

    // if that same workflow runs completes 2 hours ago, the constraint check will be satisfied
    store.setStop(pid1, nowSec - TimeUnit.HOURS.toSeconds(2), ProgramRunStatus.COMPLETED,
                  AppFabricTestHelper.createSourceId(++sourceId));
    assertSatisfied(true, lastRunConstraint.check(schedule, constraintContext));

    // a RUNNING workflow, started 2 hours ago will fail the constraint check
    startTime = nowSec - TimeUnit.HOURS.toSeconds(2);
    setStartAndRunning(store, pid2, startTime);
    assertSatisfied(false, lastRunConstraint.check(schedule, constraintContext));

    // if that same workflow run fails 1 minute ago, the constraint check will be satisfied
    store.setStop(pid2, nowSec - TimeUnit.MINUTES.toSeconds(1), ProgramRunStatus.FAILED,
                  AppFabricTestHelper.createSourceId(++sourceId));
    assertSatisfied(true, lastRunConstraint.check(schedule, constraintContext));

    // similarly, a KILLED workflow, started 2 hours ago will also fail the constraint check
    startTime = nowSec - TimeUnit.HOURS.toSeconds(2);
    setStartAndRunning(store, pid3, startTime);
    assertSatisfied(false, lastRunConstraint.check(schedule, constraintContext));
    store.setStop(pid3, nowSec - TimeUnit.MINUTES.toSeconds(1), ProgramRunStatus.KILLED,
                  AppFabricTestHelper.createSourceId(++sourceId));
    assertSatisfied(true, lastRunConstraint.check(schedule, constraintContext));

    // a RUNNING workflow, started 2 hours ago will fail the constraint check
    startTime = nowSec - TimeUnit.HOURS.toSeconds(2);
    setStartAndRunning(store, pid4, startTime);
    assertSatisfied(false, lastRunConstraint.check(schedule, constraintContext));

    // if that same workflow runs completes 1 minute ago, the constraint check will not be satisfied
    store.setStop(pid4, nowSec - TimeUnit.MINUTES.toSeconds(1), ProgramRunStatus.COMPLETED,
                  AppFabricTestHelper.createSourceId(++sourceId));
    assertSatisfied(false, lastRunConstraint.check(schedule, constraintContext));
  }

  private void assertSatisfied(boolean expectSatisfied, ConstraintResult constraintResult) {
    if (expectSatisfied) {
      Assert.assertEquals(ConstraintResult.SATISFIED, constraintResult);
    } else {
      Assert.assertNotEquals(ConstraintResult.SATISFIED, constraintResult.getSatisfiedState());
    }
  }
}
