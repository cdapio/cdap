/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.AppWithServicesAndWorker;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.gateway.handlers.ProgramLifecycleHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.test.SlowTests;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ProgramLifecycleHttpHandler}
 */
public class ProgramRunLimitTest extends AppFabricTestBase {
  private static final String STOPPED = "STOPPED";
  private static final String RUNNING = "RUNNING";

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration cConf = createBasicCConf();
    // we enable Kerberos for these unit tests, so we can test namespace group permissions (see testDataDirCreation).
    cConf.setInt(Constants.AppFabric.MAX_CONCURRENT_RUNS, 2);
    initializeAndStartServices(cConf, null);
  }

  @Category(SlowTests.class)
  @Test
  public void testConcurrentProgramRunLimit() throws Exception {
    // deploy, check the status
    deploy(AppWithServicesAndWorker.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);

    ApplicationId appId = new ApplicationId(TEST_NAMESPACE1, AppWithServicesAndWorker.NAME);
    ProgramId noOpService = appId.service(AppWithServicesAndWorker.NO_OP_SERVICE);
    ProgramId pingService = appId.service(AppWithServicesAndWorker.PING_SERVICE);
    ProgramId noOpWorker = appId.worker(AppWithServicesAndWorker.NO_OP_WORKER);

    // all programs are stopped initially
    Assert.assertEquals(STOPPED, getProgramStatus(noOpService));
    Assert.assertEquals(STOPPED, getProgramStatus(pingService));
    Assert.assertEquals(STOPPED, getProgramStatus(noOpWorker));

    // start both services and check the status
    startProgram(noOpService);
    waitState(noOpService, RUNNING);

    startProgram(pingService);
    waitState(pingService, RUNNING);

    // start the worker and check that it is rejected
    startProgram(noOpWorker, 409);
    Tasks.waitFor(1, () -> getProgramRuns(noOpWorker, ProgramRunStatus.ALL).size(), 10, TimeUnit.SECONDS);
    Assert.assertEquals(ProgramRunStatus.REJECTED, getProgramRuns(noOpWorker, ProgramRunStatus.ALL).get(0).getStatus());

    // stop one service
    stopProgram(noOpService);
    waitState(noOpService, STOPPED);

    // starting the worker should now succeed
    startProgram(noOpWorker, 200);
    Tasks.waitFor(1, () -> getProgramRuns(noOpWorker, ProgramRunStatus.COMPLETED).size(), 10, TimeUnit.SECONDS);

    // stop other service
    stopProgram(pingService);
    waitState(pingService, STOPPED);
  }
}
