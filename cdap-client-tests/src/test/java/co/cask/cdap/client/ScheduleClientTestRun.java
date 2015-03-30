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

package co.cask.cdap.client;

import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.app.FakeWorkflow;
import co.cask.cdap.client.app.PingService;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.XSlowTests;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

/**
 * Tests for {@link co.cask.cdap.client.ServiceClient}.
 */
@Category(XSlowTests.class)
public class ScheduleClientTestRun extends ClientTestBase {

  private ScheduleClient scheduleClient;
  private ProgramClient programClient;
  private ApplicationClient appClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();

    appClient = new ApplicationClient(clientConfig);
    scheduleClient = new ScheduleClient(clientConfig);
    programClient = new ProgramClient(clientConfig);

    appClient.deploy(createAppJarFile(FakeApp.class));
    programClient.start(FakeApp.NAME, ProgramType.SERVICE, PingService.NAME);
    assertProgramRunning(programClient, FakeApp.NAME, ProgramType.SERVICE, PingService.NAME);
  }

  @After
  public void tearDown() throws Throwable {
    programClient.stop(FakeApp.NAME, ProgramType.SERVICE, PingService.NAME);
    assertProgramStopped(programClient, FakeApp.NAME, ProgramType.SERVICE, PingService.NAME);
    appClient.delete(FakeApp.NAME);
  }

  @Test
  public void testAll() throws Exception {
    List<ScheduleSpecification> list = scheduleClient.list(FakeApp.NAME, FakeWorkflow.NAME);
    Assert.assertEquals(2, list.size());

    TimeSchedule schedule;
    StreamSizeSchedule streamSchedule;
    if (list.get(0).getSchedule() instanceof TimeSchedule) {
      schedule = (TimeSchedule) list.get(0).getSchedule();
      streamSchedule = (StreamSizeSchedule) list.get(1).getSchedule();
    } else {
      streamSchedule = (StreamSizeSchedule) list.get(0).getSchedule();
      schedule = (TimeSchedule) list.get(1).getSchedule();
    }

    Assert.assertEquals(FakeApp.SCHEDULE_NAME, schedule.getName());
    Assert.assertEquals(FakeApp.SCHEDULE_CRON, schedule.getCronEntry());

    Assert.assertEquals(FakeApp.STREAM_SCHEDULE_NAME, streamSchedule.getName());
    Assert.assertEquals(FakeApp.STREAM_NAME, streamSchedule.getStreamName());
    Assert.assertEquals(FakeApp.STREAM_TRIGGER_MB, streamSchedule.getDataTriggerMB());

    String status = scheduleClient.getStatus(FakeApp.NAME, FakeApp.SCHEDULE_NAME);
    Assert.assertEquals("SCHEDULED", status);

    scheduleClient.suspend(FakeApp.NAME, FakeApp.SCHEDULE_NAME);
    status = scheduleClient.getStatus(FakeApp.NAME, FakeApp.SCHEDULE_NAME);
    Assert.assertEquals("SUSPENDED", status);

    scheduleClient.resume(FakeApp.NAME, FakeApp.SCHEDULE_NAME);
    status = scheduleClient.getStatus(FakeApp.NAME, FakeApp.SCHEDULE_NAME);
    Assert.assertEquals("SCHEDULED", status);
  }
}
