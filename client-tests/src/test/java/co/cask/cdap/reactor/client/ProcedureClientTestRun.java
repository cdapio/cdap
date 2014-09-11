/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.reactor.client;

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.ProcedureClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.reactor.client.app.FakeApp;
import co.cask.cdap.reactor.client.app.FakeProcedure;
import co.cask.cdap.reactor.client.common.ClientTestBase;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;

/**
 * Test for {@link ProcedureClient}.
 */
@Category(XSlowTests.class)
public class ProcedureClientTestRun extends ClientTestBase {

  private static final Gson GSON = new Gson();

  private ApplicationClient appClient;
  private ProcedureClient procedureClient;
  private ProgramClient programClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    appClient = new ApplicationClient(clientConfig);
    procedureClient = new ProcedureClient(clientConfig);
    programClient = new ProgramClient(clientConfig);
  }

  @Test
  public void testAll() throws Exception {
    // deploy app
    File jarFile = createAppJarFile(FakeApp.class);
    appClient.deploy(jarFile);

    // check procedure list
    verifyProgramNames(FakeApp.PROCEDURES, procedureClient.list());

    // start procedure
    programClient.start(FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);

    // wait for procedure to start
    assertProgramRunning(programClient, FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);

    // call procedure
    String result = procedureClient.call(FakeApp.NAME, FakeProcedure.NAME, FakeProcedure.METHOD_NAME,
                                         ImmutableMap.of("customer", "joe"));
    Assert.assertEquals(GSON.toJson(ImmutableMap.of("customer", "realjoe")), result);

    // stop procedure
    programClient.stop(FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);
    assertProgramStopped(programClient, FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);

    appClient.delete(FakeApp.NAME);
    Assert.assertEquals(0, appClient.list().size());
  }
}
