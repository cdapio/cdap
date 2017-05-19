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
import org.junit.Ignore;
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
    List<ScheduleDetail> list = scheduleClient.list(workflow);
    Assert.assertEquals(2, list.size());

    ScheduleDetail timeSchedule;
    ScheduleDetail streamSchedule;
    if (list.get(0).getTrigger() instanceof ProtoTrigger.TimeTrigger) {
      timeSchedule = list.get(0);
      streamSchedule = list.get(1);
    } else {
      streamSchedule = list.get(0);
      timeSchedule = list.get(1);
    }
    ProtoTrigger.TimeTrigger timeTrigger = (ProtoTrigger.TimeTrigger) timeSchedule.getTrigger();
    ProtoTrigger.StreamSizeTrigger streamSizeTrigger = (ProtoTrigger.StreamSizeTrigger) streamSchedule.getTrigger();

    Assert.assertEquals(FakeApp.TIME_SCHEDULE_NAME, timeSchedule.getName());

    Assert.assertEquals(FakeApp.SCHEDULE_CRON, timeTrigger.getCronExpression());

    Assert.assertEquals(FakeApp.STREAM_SCHEDULE_NAME, streamSchedule.getName());
    Assert.assertEquals(FakeApp.STREAM_NAME, streamSizeTrigger.getStream().getStream());
    Assert.assertEquals(FakeApp.STREAM_TRIGGER_MB, streamSizeTrigger.getTriggerMB());

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
  @Ignore // TODO: bring this back as part of CDAP-11516
  public void testScheduleChanges() throws Exception {
    File appJar = createAppJarFile(FakeApp.class);

    // deploy the app with time and stream size schedule
    FakeApp.AppConfig config = new FakeApp.AppConfig(true, true, null, null, null);
    appClient.deploy(namespace, appJar, config);
    // now there should be two schedule
    List<ScheduleDetail> list = scheduleClient.list(workflow);
    Assert.assertEquals(2, list.size());

    // re-deploy the app with only time schedule i.e. we deleted the stream size schedule
    config = new FakeApp.AppConfig(true, false, null, null, null);
    appClient.deploy(namespace, appJar, config);
    // now there should be one schedule
    list = scheduleClient.list(workflow);
    Assert.assertEquals(1, list.size());

    // Try to redeploy the app with stream size schedule and with the name of existing time schedule i.e we are trying
    // to change the schedule type
    config = new FakeApp.AppConfig(false, true, null, FakeApp.TIME_SCHEDULE_NAME, null);
    appClient.deploy(namespace, appJar, config);

    // Try to change the schedule type from stream size to time again
    config = new FakeApp.AppConfig(true, false, FakeApp.TIME_SCHEDULE_NAME, null, null);
    appClient.deploy(namespace, appJar, config);
    list = scheduleClient.list(workflow);
    Assert.assertEquals(1, list.size());

    // test updating the schedule cron
    config = new FakeApp.AppConfig(true, false, FakeApp.TIME_SCHEDULE_NAME, null, "0 2 1 1 *");
    appClient.deploy(namespace, appJar, config);
    list = scheduleClient.list(workflow);
    Assert.assertEquals(1, list.size());
    ProtoTrigger.TimeTrigger trigger = (ProtoTrigger.TimeTrigger) list.get(0).getTrigger();
    Assert.assertEquals("0 2 1 1 *", trigger.getCronExpression());
  }
}
