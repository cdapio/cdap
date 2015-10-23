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
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 */
public class RunRequirementsCheckerTest {
  private static Store store;
  private static RunRequirementsChecker requirementChecker;

  @BeforeClass
  public static void setupTestClass() {
    Injector injector = AppFabricTestHelper.getInjector();
    store = injector.getInstance(Store.class);
    requirementChecker = new RunRequirementsChecker(store);
  }

  @Test
  public void testConcurrentProgramRunsThreshold() {
    Schedule schedule = Schedules.builder("abc")
      .skipIfConcurrentProgramRunsExceed(1)
      .createTimeSchedule("* * * * *");

    Id.Program programId = Id.Program.from(Id.Namespace.DEFAULT, "app", ProgramType.WORKFLOW, "workflow");
    Assert.assertTrue(requirementChecker.checkSatisfied(programId, schedule));

    store.setStart(programId, "pid1", System.currentTimeMillis());
    Assert.assertTrue(requirementChecker.checkSatisfied(programId, schedule));

    store.setStart(programId, "pid2", System.currentTimeMillis());
    Assert.assertFalse(requirementChecker.checkSatisfied(programId, schedule));

    store.setStop(programId, "pid1", System.currentTimeMillis(), ProgramRunStatus.FAILED);
    Assert.assertTrue(requirementChecker.checkSatisfied(programId, schedule));

    store.setStop(programId, "pid2", System.currentTimeMillis(), ProgramRunStatus.KILLED);
    Assert.assertTrue(requirementChecker.checkSatisfied(programId, schedule));
  }
}
