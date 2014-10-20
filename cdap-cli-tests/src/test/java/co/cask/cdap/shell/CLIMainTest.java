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

package co.cask.cdap.shell;

import co.cask.cdap.client.DatasetTypeClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.exception.ProgramNotFoundException;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.shell.app.FakeApp;
import co.cask.cdap.shell.app.FakeProcedure;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import co.cask.cdap.test.standalone.StandaloneTestBase;
import com.google.common.base.Function;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Test for {@link CLIMain}.
 */
@Category(XSlowTests.class)
public class CLIMainTest extends StandaloneTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(CLIMainTest.class);

  @Test
  public void testCommands() throws Exception {
    CLIConfig cliConfig = new CLIConfig("localhost");
    CLIMain cliMain = new CLIMain(cliConfig);

    try {
      cliMain.processInput("connect fakehost", System.out);
      Assert.fail();
    } catch (IOException e) {
      // GOOD
    }
    testCommandOutputContains(cliMain, "connect localhost", "Successfully connected");
    testCommandOutputContains(cliMain, "connect http://localhost", "Successfully connected");
    testCommandOutputContains(cliMain, "connect http://localhost:10000", "Successfully connected");

    testCommandOutputNotContains(cliMain, "list apps", FakeApp.NAME);

    File appJarFile = createAppJarFile(FakeApp.class);
    testCommandOutputContains(cliMain, "deploy app " + appJarFile.getAbsolutePath(), "Successfully deployed app");
    if (!appJarFile.delete()) {
      LOG.warn("Failed to delete temporary app jar file: {}", appJarFile.getAbsolutePath());
    }

    testCommandOutputContains(cliMain, "list apps", FakeApp.NAME);
    testCommandOutputContains(cliMain, "list dataset instances", FakeApp.DS_NAME);
    testCommandOutputContains(cliMain, "list streams", FakeApp.STREAM_NAME);
    testCommandOutputContains(cliMain, "list flows", FakeApp.FLOWS.get(0));

    // test program-related commands
    String flowId = FakeApp.FLOWS.get(0);
    String qualifiedFlowId = FakeApp.NAME + "." + flowId;
    testCommandOutputContains(cliMain, "start flow " + qualifiedFlowId, "Successfully started Flow");
    testCommandOutputContains(cliMain, "stop flow " + qualifiedFlowId, "Successfully stopped Flow");
    ProgramClient programClient = new ProgramClient(cliConfig.getClientConfig());
    assertProgramStatus(programClient, FakeApp.NAME, ProgramType.FLOW, flowId, "STOPPED");
    testCommandOutputContains(cliMain, "get status flow " + qualifiedFlowId, "STOPPED");
    testCommandOutputContains(cliMain, "get history flow " + qualifiedFlowId, "STOPPED");
    testCommandOutputContains(cliMain, "get live flow " + qualifiedFlowId, flowId);

    // test stream commands
    String streamId = "sdf123";
    testCommandOutputContains(cliMain, "create stream " + streamId, "Successfully created stream");
    testCommandOutputContains(cliMain, "list streams", streamId);
    testCommandOutputNotContains(cliMain, "get stream " + streamId, "helloworld");
    testCommandOutputContains(cliMain, "send stream " + streamId + " helloworld", "Successfully send stream event");
    testCommandOutputContains(cliMain, "get stream " + streamId, "helloworld");
    testCommandOutputContains(cliMain, "truncate stream " + streamId, "Successfully truncated stream");
    testCommandOutputNotContains(cliMain, "get stream " + streamId, "helloworld");
    testCommandOutputContains(cliMain, "set stream ttl " + streamId + " 123", "Successfully set TTL of stream");
    testCommandOutputContains(cliMain, "describe stream " + streamId, "123");

    // test dataset commands
    String datasetName = "sdf123lkj";
    DatasetTypeClient datasetTypeClient = new DatasetTypeClient(cliConfig.getClientConfig());
    DatasetTypeMeta datasetType = datasetTypeClient.list().get(0);
    testCommandOutputContains(cliMain, "create dataset instance " + datasetType.getName() + " " + datasetName,
                              "Successfully created dataset");
    testCommandOutputContains(cliMain, "truncate dataset instance " + datasetName, "Successfully truncated dataset");
    testCommandOutputContains(cliMain, "delete dataset instance " + datasetName, "Successfully deleted dataset");

    // test procedure commands
    String qualifiedProcedureId = String.format("%s.%s", FakeApp.NAME, FakeProcedure.NAME);
    testCommandOutputContains(cliMain, "start procedure " + qualifiedProcedureId, "Successfully started Procedure");
    testCommandOutputContains(cliMain, "call procedure " + qualifiedProcedureId
      + " " + FakeProcedure.METHOD_NAME + " customer bob", "realbob");
    testCommandOutputContains(cliMain, "stop procedure " + qualifiedProcedureId, "Successfully stopped Procedure");

    // cleanup
    testCommandOutputContains(cliMain, "delete app " + FakeApp.NAME, "Successfully deleted app");
    testCommandOutputContains(cliMain, "delete dataset instance " + FakeApp.DS_NAME, "Successfully deleted dataset");
  }

  private File createAppJarFile(Class<?> cls) {
    return new File(AppFabricTestHelper.createAppJar(cls).toURI());
  }

  private void testCommandOutputContains(CLIMain cliMain, String command,
                                         final String expectedOutput) throws Exception {
    testCommand(cliMain, command, new Function<String, Void>() {
      @Nullable
      @Override
      public Void apply(@Nullable String output) {
        Assert.assertTrue(String.format("Expected output '%s' to contain '%s'", output, expectedOutput),
                          output != null && output.contains(expectedOutput));
        return null;
      }
    });
  }

  private void testCommandOutputNotContains(CLIMain cliMain, String command,
                                         final String expectedOutput) throws Exception {
    testCommand(cliMain, command, new Function<String, Void>() {
      @Nullable
      @Override
      public Void apply(@Nullable String output) {
        Assert.assertTrue(String.format("Expected output '%s' to not contain '%s'", output, expectedOutput),
                          output != null && !output.contains(expectedOutput));
        return null;
      }
    });
  }

  private void testCommand(CLIMain cliMain, String command, Function<String, Void> outputValidator) throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(outputStream);
    cliMain.processInput(command, printStream);
    String output = outputStream.toString();
    outputValidator.apply(output);
  }

  protected void assertProgramStatus(ProgramClient programClient, String appId, ProgramType programType,
                                     String programId, String programStatus)
    throws IOException, ProgramNotFoundException, UnAuthorizedAccessTokenException {

    String status;
    int numTries = 0;
    int maxTries = 10;
    do {
      status = programClient.getStatus(appId, programType, programId);
      numTries++;
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        // NO-OP
      }
    } while (!status.equals(programStatus) && numTries <= maxTries);
    Assert.assertEquals(programStatus, status);
  }


}
