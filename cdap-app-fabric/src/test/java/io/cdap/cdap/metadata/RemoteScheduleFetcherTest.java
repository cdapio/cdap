/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.AppWithSchedule;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.schedule.ScheduleNotFoundException;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * Tests for {@link RemoteScheduleFetcher}
 */
public class RemoteScheduleFetcherTest extends AppFabricTestBase {
  private static ScheduleFetcher fetcher = null;

  @BeforeClass
  public static void setUp() {
    fetcher = getInjector().getInstance(RemoteScheduleFetcher.class);
  }

  @Test(expected = ScheduleNotFoundException.class)
  public void testGetScheduleNotFound() throws Exception {
    String namespace = TEST_NAMESPACE1;
    String appName = AllProgramsApp.NAME;

    // Deploy the application.
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace);

    // Get and validate the schedule
    ScheduleId scheduleId = new ScheduleId(namespace, appName, "InvalidSchedule");
    try {
      ScheduleDetail scheduleDetail = fetcher.get(scheduleId);
    } finally {
      // Delete the application
      Assert.assertEquals(
        200,
        doDelete(getVersionedAPIPath("apps/",
                                     Constants.Gateway.API_VERSION_3_TOKEN, namespace)).getResponseCode());
    }
  }

  @Test
  public void testGetSchedule() throws Exception {
    String namespace = TEST_NAMESPACE1;
    String appName = AppWithSchedule.NAME;
    String schedule = AppWithSchedule.SCHEDULE;

    // Deploy the application with just 1 schedule on the workflow.
    Config appConfig = new AppWithSchedule.AppConfig(true, true, false);
    deploy(AppWithSchedule.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace, appConfig);

    // Get and validate the schedule
    ScheduleId scheduleId = new ScheduleId(namespace, appName, schedule);
    ScheduleDetail scheduleDetail = fetcher.get(scheduleId);
    Assert.assertEquals(schedule, scheduleDetail.getName());

    // Delete the application
    Assert.assertEquals(
      200,
      doDelete(getVersionedAPIPath("apps/",
                                   Constants.Gateway.API_VERSION_3_TOKEN, namespace)).getResponseCode());
  }

  @Test
  public void testListSchedules() throws Exception {
    String namespace = TEST_NAMESPACE1;
    String appName = AppWithSchedule.NAME;

    // Deploy the application with 2 schedules on the workflow
    Config appConfig = new AppWithSchedule.AppConfig(true, true, true);
    deploy(AppWithSchedule.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace, appConfig);

    // Get and validate the schedule
    ProgramId programId = new ProgramId(namespace,
                                        appName,
                                        ProgramType.WORKFLOW,
                                        AppWithSchedule.WORKFLOW_NAME);
    List<ScheduleDetail> scheduleList = fetcher.list(programId);
    Assert.assertEquals(2, scheduleList.size());
    Assert.assertEquals(AppWithSchedule.SCHEDULE, scheduleList.get(0).getName());
    Assert.assertEquals(AppWithSchedule.SCHEDULE_2, scheduleList.get(1).getName());

    // Delete the application
    Assert.assertEquals(
      200,
      doDelete(getVersionedAPIPath("apps/",
                                   Constants.Gateway.API_VERSION_3_TOKEN, namespace)).getResponseCode());
  }
}
