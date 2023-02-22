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
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.runtime.schedule.queue.SimpleJob;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.WorkflowId;
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
  private static final ArtifactId ARTIFACT_ID = TEST_NS.artifact("test", "1.0").toApiArtifactId();
  private static final WorkflowId WORKFLOW_ID = APP_ID.workflow("wf1");
  private static final ProgramReference WORKFLOW_REF = APP_ID.getAppReference().program(ProgramType.WORKFLOW,
                                                                                        "wf1");
  private static final DatasetId DATASET_ID = TEST_NS.dataset("pfs1");

  private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();
  private int sourceId;

  private void setStartAndRunning(Store store, ProgramRunId id) {
    setStartAndRunning(store, id, ImmutableMap.of(), ImmutableMap.of());
  }

  private void setStartAndRunning(Store store, ProgramRunId id,
                                  Map<String, String> runtimeArgs, Map<String, String> systemArgs) {
    if (!systemArgs.containsKey(SystemArguments.PROFILE_NAME)) {
      systemArgs = ImmutableMap.<String, String>builder()
        .putAll(systemArgs)
        .put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName())
        .build();
    }
    long startTime = RunIds.getTime(id.getRun(), TimeUnit.SECONDS);
    store.setProvisioning(id, runtimeArgs, systemArgs, AppFabricTestHelper.createSourceId(++sourceId), ARTIFACT_ID);
    store.setProvisioned(id, 0, AppFabricTestHelper.createSourceId(++sourceId));
    store.setStart(id, null, systemArgs, AppFabricTestHelper.createSourceId(++sourceId));
    store.setRunning(id, startTime + 1, null, AppFabricTestHelper.createSourceId(++sourceId));
  }
  
  @Test
  public void testLastRunConstraint() {
    Store store = AppFabricTestHelper.getInjector().getInstance(Store.class);
    try {
      long now = System.currentTimeMillis();
      long nowSec = TimeUnit.MILLISECONDS.toSeconds(now);
      ProgramSchedule schedule = new ProgramSchedule("SCHED1", "one partition schedule", WORKFLOW_REF,
                                                     ImmutableMap.of("prop3", "abc"),
                                                     new PartitionTrigger(DATASET_ID, 1),
                                                     ImmutableList.of());
      SimpleJob job = new SimpleJob(schedule, 0, now, Collections.emptyList(), Job.State.PENDING_TRIGGER, 0L);

      // require 1 hour since last run
      LastRunConstraint lastRunConstraint = new LastRunConstraint(1, TimeUnit.HOURS);
      ConstraintContext constraintContext = new ConstraintContext(job, now, store);

      // there's been no runs, so the constraint is satisfied by default
      assertSatisfied(true, lastRunConstraint.check(schedule, constraintContext));

      // a RUNNING workflow, started 3 hours ago will fail the constraint check
      ProgramRunId pid1 = WORKFLOW_ID.run(RunIds.generate(nowSec - TimeUnit.HOURS.toSeconds(3)).getId());
      Map<String, String> systemArgs = ImmutableMap.of(ProgramOptionConstants.SCHEDULE_NAME, schedule.getName());
      setStartAndRunning(store, pid1, EMPTY_MAP, systemArgs);
      assertSatisfied(false, lastRunConstraint.check(schedule, constraintContext));

      // a SUSPENDED workflow started 3 hours ago will also fail the constraint check
      store.setSuspend(pid1, AppFabricTestHelper.createSourceId(++sourceId), -1);
      assertSatisfied(false, lastRunConstraint.check(schedule, constraintContext));
      store.setResume(pid1, AppFabricTestHelper.createSourceId(++sourceId), -1);
      assertSatisfied(false, lastRunConstraint.check(schedule, constraintContext));

      // if that same workflow runs completes 2 hours ago, the constraint check will be satisfied
      store.setStop(pid1, nowSec - TimeUnit.HOURS.toSeconds(2), ProgramRunStatus.COMPLETED,
                    AppFabricTestHelper.createSourceId(++sourceId));
      assertSatisfied(true, lastRunConstraint.check(schedule, constraintContext));

      // a RUNNING workflow, started 2 hours ago will fail the constraint check
      ProgramRunId pid2 = WORKFLOW_ID.run(RunIds.generate(nowSec - TimeUnit.HOURS.toSeconds(2)).getId());
      setStartAndRunning(store, pid2);
      assertSatisfied(false, lastRunConstraint.check(schedule, constraintContext));

      // if that same workflow run fails 1 minute ago, the constraint check will be satisfied
      store.setStop(pid2, nowSec - TimeUnit.MINUTES.toSeconds(1), ProgramRunStatus.FAILED,
                    AppFabricTestHelper.createSourceId(++sourceId));
      assertSatisfied(true, lastRunConstraint.check(schedule, constraintContext));

      // similarly, a KILLED workflow, started 2 hours ago will also fail the constraint check
      ProgramRunId pid3 = WORKFLOW_ID.run(RunIds.generate(nowSec - TimeUnit.HOURS.toSeconds(2)).getId());
      setStartAndRunning(store, pid3);
      assertSatisfied(false, lastRunConstraint.check(schedule, constraintContext));
      store.setStop(pid3, nowSec - TimeUnit.MINUTES.toSeconds(1), ProgramRunStatus.KILLED,
                    AppFabricTestHelper.createSourceId(++sourceId));
      assertSatisfied(true, lastRunConstraint.check(schedule, constraintContext));

      // a RUNNING workflow, started 2 hours ago will fail the constraint check
      ProgramRunId pid4 = WORKFLOW_ID.run(RunIds.generate(nowSec - TimeUnit.HOURS.toSeconds(2)).getId());
      setStartAndRunning(store, pid4);
      assertSatisfied(false, lastRunConstraint.check(schedule, constraintContext));

      // if that same workflow runs completes 1 minute ago, the constraint check will not be satisfied
      store.setStop(pid4, nowSec - TimeUnit.MINUTES.toSeconds(1), ProgramRunStatus.COMPLETED,
                    AppFabricTestHelper.createSourceId(++sourceId));
      assertSatisfied(false, lastRunConstraint.check(schedule, constraintContext));
    } finally {
      AppFabricTestHelper.shutdown();
    }
  }

  private void assertSatisfied(boolean expectSatisfied, ConstraintResult constraintResult) {
    if (expectSatisfied) {
      Assert.assertEquals(ConstraintResult.SATISFIED, constraintResult);
    } else {
      Assert.assertNotEquals(ConstraintResult.SATISFIED, constraintResult.getSatisfiedState());
    }
  }
}
