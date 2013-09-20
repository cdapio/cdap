package com.continuuity.internal.app.scheduler;

import com.continuuity.app.Id;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.ProgramId;
import com.continuuity.app.services.ProgramRunRecord;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.test.internal.TestHelper;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import junit.framework.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Schedule pause and run
 */
public class SchedulePauseResumeTests {

  @Test
  public void testRunner() throws Exception {
    AppFabricServer appFabricServer = TestHelper.getInjector().getInstance(AppFabricServer.class);

    try {
      appFabricServer.startAndWait();
      AppFabricService.Iface appFabricService = TestHelper.getInjector().getInstance(AppFabricService.Iface.class);

      TestHelper.deployApplication(appFabricService, new LocalLocationFactory(),
                                   Id.Account.from("schedulePauseTester"), new AuthToken("token"), "SampleApplication",
                                   "SampleApp", SampleApplication.class);

      ProgramId id  = new ProgramId("schedulePauseTester", "SampleApp", "SampleWorkflow");
      int count = 0;
      int workflowRunCount = 0;
      //Wait for 90 seconds or until there is one run of the workflow
      while (count <= 90 && workflowRunCount == 0) {
        count++;
        List<ProgramRunRecord> result = appFabricService.getHistory(id);
        workflowRunCount = result.size();
        TimeUnit.SECONDS.sleep(1L);
      }
      Assert.assertTrue(workflowRunCount >= 1);

    } finally {
      appFabricServer.stopAndWait();
    }
  }
}
