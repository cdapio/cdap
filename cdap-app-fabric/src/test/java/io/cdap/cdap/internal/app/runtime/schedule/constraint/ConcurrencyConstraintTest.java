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
 * Tests for {@link ConcurrencyConstraint}.
 */
public class ConcurrencyConstraintTest {
  private static final NamespaceId TEST_NS = new NamespaceId("ConcurrencyConstraintTest");
  private static final ApplicationId APP_ID = TEST_NS.app("app1");
  private static final ArtifactId ARTIFACT_ID = TEST_NS.artifact("test", "1.0").toApiArtifactId();
  private static final WorkflowId WORKFLOW_ID = APP_ID.workflow("wf1");
  private static final ProgramReference WORKFLOW_REF = APP_ID.getAppReference().program(ProgramType.WORKFLOW,
                                                                                        "wf1");
  private static final DatasetId DATASET_ID = TEST_NS.dataset("pfs1");

  private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();
  
  private int sourceId;
  
  private void setStartAndRunning(Store store, ProgramRunId id) {
    setStartAndRunning(store, id, EMPTY_MAP, EMPTY_MAP);

  }

  private void setStartAndRunning(Store store, ProgramRunId id,
                                  Map<String, String> runtimeArgs,
                                  Map<String, String> systemArgs) {
    setProvisioning(store, id, runtimeArgs, systemArgs);
    long startTime = RunIds.getTime(id.getRun(), TimeUnit.SECONDS);
    store.setProvisioned(id, 0, AppFabricTestHelper.createSourceId(++sourceId));
    store.setStart(id, null, systemArgs, AppFabricTestHelper.createSourceId(++sourceId));
    store.setRunning(id, startTime + 1, null, AppFabricTestHelper.createSourceId(++sourceId));
  }

  private void setProvisioning(Store store, ProgramRunId id,
                                  Map<String, String> runtimeArgs,
                                  Map<String, String> systemArgs) {
    if (!systemArgs.containsKey(SystemArguments.PROFILE_NAME)) {
      systemArgs = ImmutableMap.<String, String>builder()
        .putAll(systemArgs)
        .put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName())
        .build();
    }
    store.setProvisioning(id, runtimeArgs, systemArgs, AppFabricTestHelper.createSourceId(++sourceId), ARTIFACT_ID);
  }

  @Test
  public void testMaxConcurrentRuns() {
    Store store = AppFabricTestHelper.getInjector().getInstance(Store.class);
    try {
      long now = System.currentTimeMillis();
      ProgramSchedule schedule = new ProgramSchedule("SCHED1", "one partition schedule", WORKFLOW_REF,
                                                     ImmutableMap.of("prop3", "abc"),
                                                     new PartitionTrigger(DATASET_ID, 1),
                                                     ImmutableList.of());
      SimpleJob job = new SimpleJob(schedule, 0, now, Collections.emptyList(), Job.State.PENDING_TRIGGER, 0L);

      ConcurrencyConstraint concurrencyConstraint = new ConcurrencyConstraint(2);
      ConstraintContext constraintContext = new ConstraintContext(job, now, store);

      assertSatisfied(true, concurrencyConstraint.check(schedule, constraintContext));

      ProgramRunId pid1 = WORKFLOW_ID.run(RunIds.generate().getId());
      ProgramRunId pid2 = WORKFLOW_ID.run(RunIds.generate().getId());
      ProgramRunId pid3 = WORKFLOW_ID.run(RunIds.generate().getId());

      // add a run for the schedule
      Map<String, String> systemArgs = ImmutableMap.of(ProgramOptionConstants.SCHEDULE_NAME, schedule.getName());
      setStartAndRunning(store, pid1, EMPTY_MAP, systemArgs);
      assertSatisfied(true, concurrencyConstraint.check(schedule, constraintContext));

      // add a run for the program from a different schedule. Since there are now 2 running instances of the
      // workflow (regardless of the schedule name), the constraint is not met
      systemArgs = ImmutableMap.of(ProgramOptionConstants.SCHEDULE_NAME, "not" + schedule.getName());
      setStartAndRunning(store, pid2, EMPTY_MAP, systemArgs);
      assertSatisfied(false, concurrencyConstraint.check(schedule, constraintContext));

      // add a run for the program that wasn't from a schedule
      // there are now three concurrent runs, so the constraint will not be met
      setStartAndRunning(store, pid3);
      assertSatisfied(false, concurrencyConstraint.check(schedule, constraintContext));

      // stop the first program; constraint will not be satisfied as there are still 2 running
      store.setStop(pid1, System.currentTimeMillis(), ProgramRunStatus.COMPLETED,
                    AppFabricTestHelper.createSourceId(++sourceId));
      assertSatisfied(false, concurrencyConstraint.check(schedule, constraintContext));

      // suspending/resuming the workflow doesn't reduce its concurrency count
      store.setSuspend(pid3, AppFabricTestHelper.createSourceId(++sourceId), -1);
      assertSatisfied(false, concurrencyConstraint.check(schedule, constraintContext));
      store.setResume(pid3, AppFabricTestHelper.createSourceId(++sourceId), -1);
      assertSatisfied(false, concurrencyConstraint.check(schedule, constraintContext));

      // but the constraint will be satisfied with it completes, as there is only 1 remaining RUNNING
      store.setStop(pid3, System.currentTimeMillis(), ProgramRunStatus.KILLED,
                    AppFabricTestHelper.createSourceId(++sourceId));
      assertSatisfied(true, concurrencyConstraint.check(schedule, constraintContext));

      // add a run in provisioning state, constraint will not be satisfied since active runs increased to 2
      ProgramRunId pid4 = WORKFLOW_ID.run(RunIds.generate().getId());
      setProvisioning(store, pid4, Collections.emptyMap(), Collections.emptyMap());
      assertSatisfied(false, concurrencyConstraint.check(schedule, constraintContext));

      // stop the provisioning run, constraint will be satisfied since active runs decreased to 1
      store.setStop(pid4, System.currentTimeMillis(), ProgramRunStatus.FAILED,
                    AppFabricTestHelper.createSourceId(++sourceId));
      assertSatisfied(true, concurrencyConstraint.check(schedule, constraintContext));

      // stopping the last running workflow will also satisfy the constraint
      store.setStop(pid2, System.currentTimeMillis(), ProgramRunStatus.FAILED,
                    AppFabricTestHelper.createSourceId(++sourceId));
      assertSatisfied(true, concurrencyConstraint.check(schedule, constraintContext));
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
