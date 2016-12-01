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
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

/**
 */
public class RunConstraintsCheckerTest {
  private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();
  private static Store store;
  private static RunConstraintsChecker requirementChecker;

  @BeforeClass
  public static void setupTestClass() {
    Injector injector = AppFabricTestHelper.getInjector();
    store = injector.getInstance(Store.class);
    requirementChecker = new RunConstraintsChecker(store);
  }

  @Test
  public void testMaxConcurrentRuns() {
    Schedule schedule = Schedules.builder("abc")
      .setMaxConcurrentRuns(2)
      .createTimeSchedule("* * * * *");

    ProgramId programId = NamespaceId.DEFAULT.app("app").workflow("workflow");
    Assert.assertTrue(requirementChecker.checkSatisfied(programId, schedule));

    // add a run for the schedule
    Map<String, String> systemArgs = ImmutableMap.of(ProgramOptionConstants.SCHEDULE_NAME, schedule.getName());
    store.setStart(programId, "pid1", System.currentTimeMillis(), null, EMPTY_MAP, systemArgs);
    Assert.assertTrue(requirementChecker.checkSatisfied(programId, schedule));

    // add a run for the program from a different schedule
    systemArgs = ImmutableMap.of(ProgramOptionConstants.SCHEDULE_NAME, "not" + schedule.getName());
    store.setStart(programId, "pid2", System.currentTimeMillis(), null, EMPTY_MAP, systemArgs);
    Assert.assertTrue(requirementChecker.checkSatisfied(programId, schedule));

    // add a run for the program that wasn't from a schedule
    store.setStart(programId, "pid3", System.currentTimeMillis(), null, EMPTY_MAP, EMPTY_MAP);
    Assert.assertTrue(requirementChecker.checkSatisfied(programId, schedule));

    // now add another run for the schedule, constraints should not be satisfied now
    systemArgs = ImmutableMap.of(ProgramOptionConstants.SCHEDULE_NAME, schedule.getName());
    store.setStart(programId, "pid4", System.currentTimeMillis(), null, EMPTY_MAP, systemArgs);
    Assert.assertFalse(requirementChecker.checkSatisfied(programId, schedule));

    // stop the first program, constraints should be satisfied now
    store.setStop(programId, "pid1", System.currentTimeMillis(), ProgramRunStatus.FAILED);
    Assert.assertTrue(requirementChecker.checkSatisfied(programId, schedule));

    store.setStop(programId, "pid4", System.currentTimeMillis(), ProgramRunStatus.KILLED);
    Assert.assertTrue(requirementChecker.checkSatisfied(programId, schedule));
  }
}
