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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.common.collect.ImmutableMap;
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
  public static void beforeClass() throws Throwable {
    AppFabricTestBase.beforeClass();
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
    timeScheduler.resumeProgramSchedule(sched);
    long startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) + TimeUnit.HOURS.toSeconds(1);
    List<ScheduledRuntime> nextRuntimes = timeScheduler.getAllScheduledRunTimes(PROG1_ID, SchedulableProgramType.WORKFLOW,
                                                                                startTime, startTime + TimeUnit.HOURS.toSeconds(2));
    // since the time range is inclusive at the start and exclusive at the end,
    // in a 2-hour time range, the number of times that the schedule will be triggered is 2 hour / 5 min = 24
    Assert.assertEquals(TimeUnit.HOURS.toSeconds(2) / TimeUnit.MINUTES.toSeconds(5),
      nextRuntimes.size());
  }
}
