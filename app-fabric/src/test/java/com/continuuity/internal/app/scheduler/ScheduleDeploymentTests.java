package com.continuuity.internal.app.scheduler;

import com.continuuity.app.Id;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.EntityType;
import com.continuuity.app.services.ProgramId;
import com.continuuity.app.services.ScheduleId;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.test.internal.TestHelper;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import junit.framework.Assert;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class ScheduleDeploymentTests {

  @Test
  public void testDeployment() throws Exception {

    AppFabricServer appFabricServer = TestHelper.getInjector().getInstance(AppFabricServer.class);
    AuthToken token = new AuthToken("token");
    try {
      appFabricServer.startAndWait();
      AppFabricService.Iface appFabricService = appFabricServer.getService();
      appFabricService.reset(TestHelper.DUMMY_AUTH_TOKEN, "developer");
      TestHelper.deployApplication(appFabricService, new LocalLocationFactory(),
                                   Id.Account.from("developer"), token, "SampleApplication",
                                   "SampleApp", SampleApplication.class);

      ProgramId id  = new ProgramId("developer", "SampleApp", "SampleWorkflow");
      id.setType(EntityType.WORKFLOW);

      List<ScheduleId> schedules = appFabricService.getSchedules(TestHelper.DUMMY_AUTH_TOKEN, id);
      Assert.assertEquals(1, schedules.size());

      //deploy application with same name and no schedule.
      TestHelper.deployApplication(appFabricService, new LocalLocationFactory(),
                                   Id.Account.from("developer"), token, "SampleApplication",
                                   "SampleApp", SampleApplicationNoSchedule.class);

      schedules = appFabricService.getSchedules(TestHelper.DUMMY_AUTH_TOKEN, id);
      Assert.assertEquals(0, schedules.size());
    }
    finally {
      appFabricServer.stopAndWait();
    }
  }

}
