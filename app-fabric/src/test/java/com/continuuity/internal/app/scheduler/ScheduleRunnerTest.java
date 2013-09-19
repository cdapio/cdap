package com.continuuity.internal.app.scheduler;

import com.continuuity.app.Id;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.test.internal.TestHelper;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import junit.framework.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests if Workflows are scheduled.
 */
public class ScheduleRunnerTest {

  @Test
  public void testRunner() throws Exception {
    AppFabricServer appFabricServer = TestHelper.getInjector().getInstance(AppFabricServer.class);

    Assert.assertEquals(0, SampleApplication.hasRun);
    appFabricServer.startAndWait();

    AppFabricService.Iface appFabricService = TestHelper.getInjector().getInstance(AppFabricService.Iface.class);

    TestHelper.deployApplication(appFabricService, new LocalLocationFactory(),
                                 Id.Account.from("developer"), new AuthToken("token"), "SampleApplication",
                                 "app", SampleApplication.class);

    TimeUnit.SECONDS.sleep(30);

    Assert.assertEquals(1, SampleApplication.hasRun);
    appFabricServer.stopAndWait();
  }
}
