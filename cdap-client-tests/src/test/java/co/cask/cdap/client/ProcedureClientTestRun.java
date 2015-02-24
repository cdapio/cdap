/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.client.app.FakeProcedure;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.ProgramType;
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

    // Validate that procedure calls can not be made to non-default namespaces
    String testNamespace = clientConfig.getNamespace();
    clientConfig.setNamespace("fooNamespace");
    try {
      procedureClient.call(FakeApp.NAME, FakeProcedure.NAME, FakeProcedure.METHOD_NAME,
                           ImmutableMap.of("customer", "joe"));
      Assert.fail("Procedure calls should not be supported in non-default namespaces.");
    } catch (IllegalStateException e) {
      String expectedErrMsg = "Procedure operations are only supported in the default namespace.";
      Assert.assertEquals(expectedErrMsg, e.getMessage());
    }
    // revert namespace to continue execution of test
    clientConfig.setNamespace(testNamespace);

    // Validate that procedure calls can not be in non-V2 APIs
    String testVersion = clientConfig.getApiVersion();
    clientConfig.setApiVersion(Constants.Gateway.API_VERSION_3_TOKEN);
    try {
      procedureClient.call(FakeApp.NAME, FakeProcedure.NAME, FakeProcedure.METHOD_NAME,
                           ImmutableMap.of("customer", "joe"));
      Assert.fail("Procedure calls should not be supported in non-v2 APIs.");
    } catch (IllegalStateException e) {
      String expectedErrMsg = "Procedure operations are only supported in V2 APIs.";
      Assert.assertEquals(expectedErrMsg, e.getMessage());
    }
    // revert apiVersion to continue execution of test
    clientConfig.setApiVersion(testVersion);

    // stop procedure
    programClient.stop(FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);
    assertProgramStopped(programClient, FakeApp.NAME, ProgramType.PROCEDURE, FakeProcedure.NAME);

    appClient.delete(FakeApp.NAME);
    Assert.assertEquals(0, appClient.list().size());
  }
}
