package com.continuuity.internal.app.scheduler;

import com.continuuity.app.Id;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.EntityType;
import com.continuuity.app.services.ProgramId;
import com.continuuity.app.services.ProgramRunRecord;
import com.continuuity.app.services.ScheduleId;
import com.continuuity.app.store.Store;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.internal.app.store.MDSBasedStore;
import com.continuuity.test.internal.TestHelper;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.google.common.collect.Maps;
import junit.framework.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test schedule run, pause and resume.
 */
public class ScheduleRunPauseResumeTests {

  @Test
  public void testRunner() throws Exception {
    AppFabricServer appFabricServer = TestHelper.getInjector().getInstance(AppFabricServer.class);
    AuthToken token = new AuthToken("token");
    try {
      appFabricServer.startAndWait();
      AppFabricService.Iface appFabricService = TestHelper.getInjector().getInstance(AppFabricService.Iface.class);

      appFabricService.reset(TestHelper.DUMMY_AUTH_TOKEN, "developer");

      TestHelper.deployApplication(appFabricService, new LocalLocationFactory(),
                                   Id.Account.from("developer"), token, "SampleApplication",
                                   "SampleApp", SampleApplication.class);

      ProgramId id  = new ProgramId("developer", "SampleApp", "SampleWorkflow");
      id.setType(EntityType.WORKFLOW);

      int count = 0;
      int workflowRunCount = 0;
      //Wait for 10 seconds or until there is one run of the workflow
      while (count <= 10 && workflowRunCount == 0) {
        count++;
        List<ProgramRunRecord> result = appFabricService.getHistory(id);
        workflowRunCount = result.size();
        TimeUnit.SECONDS.sleep(1L);
      }
      Assert.assertTrue(workflowRunCount >= 1);

      List<ScheduleId> scheduleIds = appFabricService.getSchedules(token, id);
      Assert.assertEquals(1, scheduleIds.size());
      ScheduleId scheduleId = scheduleIds.get(0);

      appFabricService.suspendSchedule(token, scheduleId);
      TimeUnit.SECONDS.sleep(2L);

      //get the current number runs and check if after a period of time there are no new runs.
      int numWorkFlowRuns =  appFabricService.getHistory(id).size();
      TimeUnit.SECONDS.sleep(10L);

      int numWorkFlowRunsAfterWait = appFabricService.getHistory(id).size();
      Assert.assertEquals(numWorkFlowRuns, numWorkFlowRunsAfterWait);

      appFabricService.resumeSchedule(token, scheduleId);
      int numWorkflowRunAfterResume = 0;

      count = 0;
      while (count <= 10 && numWorkflowRunAfterResume == 0){
        count++;
        numWorkflowRunAfterResume = appFabricService.getHistory(id).size();
        TimeUnit.SECONDS.sleep(1L);
      }

      Assert.assertTrue(numWorkflowRunAfterResume >= 1);

      Map<String, String> args = Maps.newHashMap();
      args.put("Key1", "val1");
      args.put("Key2", "val2");
      appFabricService.storeRuntimeArguments(token, id, args);

      Store store = TestHelper.getInjector().getInstance(MDSBasedStore.class);
      Map<String, String> argsRead = store.getRunArguments(Id.Program.from("developer", "SampleApp",
                                                                           "SampleWorkflow"));

      Assert.assertEquals(2, argsRead.size());
      Assert.assertEquals("val1", argsRead.get("Key1"));
      Assert.assertEquals("val2", argsRead.get("Key2"));

      Map<String, String> emptyArgs = store.getRunArguments(Id.Program.from("Br", "Ba", "d"));
      Assert.assertEquals(0, emptyArgs.size());

    } finally {
      appFabricServer.stopAndWait();
    }
  }
}
