/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.app.FakeWorkflow;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.ProtoTrigger;
import co.cask.cdap.proto.ScheduleDetail;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.WorkflowId;
import co.cask.cdap.test.XSlowTests;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link co.cask.cdap.client.ServiceClient}.
 */
@Category(XSlowTests.class)
public class ScheduleClientTestRun extends ClientTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduleClientTestRun.class);

  private final NamespaceId namespace = NamespaceId.DEFAULT;
  private final ApplicationId app = namespace.app(FakeApp.NAME);
  private final WorkflowId workflow = app.workflow(FakeWorkflow.NAME);
  private final ScheduleId schedule = app.schedule(FakeApp.TIME_SCHEDULE_NAME);

  private ScheduleClient scheduleClient;
  private ApplicationClient appClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    appClient = new ApplicationClient(clientConfig);
    scheduleClient = new ScheduleClient(clientConfig);
    appClient.deploy(namespace, createAppJarFile(FakeApp.class));
  }

  @After
  public void tearDown() throws Throwable {
    try {
      appClient.delete(app);
    } catch (Exception e) {
      LOG.error("Error deleting app {} during test cleanup.", e);
    }
  }

  @Test
  public void testAll() throws Exception {
    List<ScheduleDetail> list = scheduleClient.listSchedules(workflow);
    Assert.assertEquals(1, list.size());

    ScheduleDetail timeSchedule = list.get(0);
    ProtoTrigger.TimeTrigger timeTrigger = (ProtoTrigger.TimeTrigger) timeSchedule.getTrigger();

    Assert.assertEquals(FakeApp.TIME_SCHEDULE_NAME, timeSchedule.getName());

    Assert.assertEquals(FakeApp.SCHEDULE_CRON, timeTrigger.getCronExpression());

    String status = scheduleClient.getStatus(schedule);
    Assert.assertEquals("SUSPENDED", status);
    status = scheduleClient.getStatus(schedule);
    Assert.assertEquals("SUSPENDED", status);

    scheduleClient.resume(schedule);
    status = scheduleClient.getStatus(schedule);
    Assert.assertEquals("SCHEDULED", status);

    scheduleClient.suspend(schedule);
    status = scheduleClient.getStatus(schedule);
    Assert.assertEquals("SUSPENDED", status);

    scheduleClient.resume(schedule);
    status = scheduleClient.getStatus(schedule);
    Assert.assertEquals("SCHEDULED", status);

    scheduleClient.suspend(schedule);
    status = scheduleClient.getStatus(schedule);
    Assert.assertEquals("SUSPENDED", status);

    scheduleClient.resume(schedule);
    List<ScheduledRuntime> scheduledRuntimes = scheduleClient.nextRuntimes(workflow);
    scheduleClient.suspend(schedule);
    Assert.assertEquals(1, scheduledRuntimes.size());
    // simply assert that its scheduled for some time in the future (or scheduled for now, but hasn't quite
    // executed yet
    Assert.assertTrue(scheduledRuntimes.get(0).getTime() >= System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1));

    try {
      scheduleClient.nextRuntimes(app.workflow("nonexistentWorkflow"));
      Assert.fail("Expected not to be able to retrieve next run times for a nonexistent workflow.");
    } catch (NotFoundException expected) {
      // expected
    }
  }

  @Test
  public void testScheduleChanges() throws Exception {
    File appJar = createAppJarFile(FakeApp.class);

    // deploy the app with time schedule
    FakeApp.AppConfig config = new FakeApp.AppConfig(true, null, null);
    appClient.deploy(namespace, appJar, config);
    // now there should be one schedule
    List<ScheduleDetail> list = scheduleClient.listSchedules(workflow);
    Assert.assertEquals(1, list.size());

    // test updating the schedule cron
    config = new FakeApp.AppConfig(true, FakeApp.TIME_SCHEDULE_NAME, "0 2 1 1 *");
    appClient.deploy(namespace, appJar, config);
    list = scheduleClient.listSchedules(workflow);
    Assert.assertEquals(1, list.size());
    ProtoTrigger.TimeTrigger trigger = (ProtoTrigger.TimeTrigger) list.get(0).getTrigger();
    Assert.assertEquals("0 2 1 1 *", trigger.getCronExpression());

    // re-deploy the app without time schedule
    config = new FakeApp.AppConfig(false, null, null);
    appClient.deploy(namespace, appJar, config);
    // now there should be no schedule
    list = scheduleClient.listSchedules(workflow);
    Assert.assertEquals(0, list.size());
  }
}
