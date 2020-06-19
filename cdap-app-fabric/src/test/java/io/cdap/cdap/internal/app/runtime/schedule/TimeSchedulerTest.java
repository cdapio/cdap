/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.WorkflowId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link TimeScheduler}
 */
public class TimeSchedulerTest extends AppFabricTestBase {

  private static final NamespaceId NS_ID = new NamespaceId("schedtest");
  private static final ApplicationId APP1_ID = NS_ID.app("app1");
  private static final WorkflowId PROG1_ID = APP1_ID.workflow("wf1");

  private static TimeScheduler timeScheduler;

  @BeforeClass
  public static void setup() throws Throwable {
    timeScheduler = getInjector().getInstance(TimeScheduler.class);
    timeScheduler.init();
    timeScheduler.start();
  }


  @Test
  public void testNextSchedule() throws Exception {
    // a schedule to be triggered every 5 minutes
    ProgramSchedule sched = new ProgramSchedule("tsched11", "two times schedule", PROG1_ID,
      ImmutableMap.of("prop2", "xx"),
      new TimeTrigger("*/5 * * * *"), Collections.emptyList());
    timeScheduler.addProgramSchedule(sched);
    // schedule is by default SUSPENDED after being added, resume it to enable the schedule
    timeScheduler.resumeProgramSchedule(sched);
    long currentTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    long startTimeInSeconds = currentTimeInSeconds + TimeUnit.HOURS.toSeconds(1);
    long endTimeInSeconds = currentTimeInSeconds + TimeUnit.HOURS.toSeconds(3);
    List<ScheduledRuntime> nextRuntimes =
      timeScheduler.getAllScheduledRunTimes(PROG1_ID, SchedulableProgramType.WORKFLOW,
                                            startTimeInSeconds, endTimeInSeconds);
    // for a scan range of 1pm to 3pm. since start time is inclusive, from 1pm tp 2pm we will have 13 schedules
    // and from 2:05 pm to 2:55pm will have 11 schedules as end time is exclusive. in total we expect 24 schedules.
    Assert.assertEquals(24, nextRuntimes.size());
  }
}
